package com.aerospike.dynafeed;


import com.aerospike.dynafeed.InventoryItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;


/**
 * Synthesizes plausible vintage fashion {@link InventoryItem}s for load generation.
 *
 * <p>Each synthesized item is assigned a {@code listingUser} drawn from the live
 * {@code user} table: the synthesizer guesses a random {@code userId} in
 * {@value #USER_ID_MIN}..{@value #USER_ID_MAX} and issues an existence check
 * against DynamoDB. If the user exists, that {@code userId} is used. If not,
 * the synthesizer guesses again, up to {@value #MAX_USER_LOOKUP_ATTEMPTS}
 * times. If no valid user is found within the attempt budget, the synthesis
 * call returns {@link Optional#empty()} and the feeder is expected to skip
 * the write.
 *
 * <p>This guess-and-check pattern is deliberately transactionless. In a real
 * marketplace, a new listing arriving on a Kafka queue references a
 * {@code userId} that almost always exists (because users are created far more
 * often than they are deleted), so a single existence check is sufficient
 * defense without the cost of a full transaction. The synthesizer reproduces
 * that workload character — multiple lightweight reads per write — which is
 * exactly the read-amplified pattern feature-store-adjacent systems exhibit.
 *
 * <p>Existence checks use a projection expression returning only the
 * {@code userId} attribute, minimizing payload size. RCU cost is still 1
 * (DynamoDB rounds up to 4KB regardless of projection), but the network
 * payload is small.
 *
 * <p>Design notes:
 * <ul>
 *   <li>{@code itemId} is a UUIDv4 string. Independent generation across
 *       clients with negligible collision probability and uniform partition
 *       key distribution.</li>
 *   <li>All random choice goes through {@link ThreadLocalRandom} so instances
 *       are safe to share across worker threads.</li>
 *   <li>Brands, colors, and product-line eligibility are coupled: Coach
 *       doesn't make dresses; Burberry's color names aren't Hermès's. The
 *       synthesizer respects these constraints so generated items look
 *       plausible to a human reviewer spot-checking the table.</li>
 *   <li>Titles are derived from attributes at creation time using the same
 *       pattern the real site presumably uses: "{Gender}'s {Decade}s {Brand}
 *       {Style} {Product} in {Condition} Condition".</li>
 * </ul>
 */
public final class ItemSynthesizer
{
    private static final Logger log = LoggerFactory.getLogger(ItemSynthesizer.class);

    private static final String USER_TABLE_NAME = "user";
    private static final long USER_ID_MIN = 1L;
    private static final long USER_ID_MAX = 6_000L;
    private static final int MAX_USER_LOOKUP_ATTEMPTS = 10;

    private final DynamoDbClient client;


    public ItemSynthesizer(DynamoDbClient client)
    {
        this.client = client;
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------


    /**
     * Generates a single synthetic InventoryItem with a fresh UUID and a
     * verified-existing {@code listingUser}. Returns {@link Optional#empty()}
     * if no valid {@code userId} could be found within the lookup budget.
     */
    public Optional<InventoryItem> synthesize( )
    {
        ThreadLocalRandom rng = ThreadLocalRandom.current( );

        // First, find a valid listingUser. Bail out if we can't.
        Optional<Long> listingUser = findExistingUserId(rng);
        if (listingUser.isEmpty( )) {
            log.info("Skipped item synthesis: no valid userId found in {} attempts " +
                "(range {}..{})", MAX_USER_LOOKUP_ATTEMPTS, USER_ID_MIN, USER_ID_MAX);
            return Optional.empty( );
        }

        // Pick a brand first — it constrains everything downstream.
        Brand brand = pick(BRANDS, rng);

        // Pick a product the brand actually makes.
        Product product = pick(brand.products, rng);

        // Gender: respect product constraints, then brand leanings.
        String gender = pickGender(brand, product, rng);

        // Year: within the brand's vintage-relevant window.
        int year = rng.nextInt(brand.earliestYear, brand.latestYear + 1);

        // Everything else is largely independent.
        String color = pick(brand.colors, rng);
        String size = pickSize(product, rng);
        String condition = pick(CONDITIONS, rng);
        BigDecimal price = synthesizePrice(brand, product, year, condition, rng);

        InventoryItem item = new InventoryItem( );
        item.setItemId(UUID.randomUUID( ).toString( ));
        item.setListingUser(listingUser.get( ));
        item.setBrand(brand.name);
        item.setCategory(product.category);
        item.setSubcategory(product.subcategory);
        item.setStyle(product.style);
        item.setGender(gender);
        item.setSize(size);
        item.setYear(String.valueOf(year));
        item.setColor(color);
        item.setCondition(condition);
        item.setPrice(price);
        item.setTitle(deriveTitle(brand, product, gender, year, condition));

        return Optional.of(item);
    }

    // ------------------------------------------------------------------
    // User existence check
    // ------------------------------------------------------------------


    /**
     * Guesses a random userId in the live user range and verifies its
     * existence in DynamoDB. Retries on miss up to the attempt budget.
     * Uses a projection expression to minimize payload, and eventually
     * consistent reads (the SDK default) since exact freshness is
     * irrelevant for this check.
     */
    private Optional<Long> findExistingUserId(ThreadLocalRandom rng)
    {
        for (int attempt = 1; attempt <= MAX_USER_LOOKUP_ATTEMPTS; attempt++) {
            long candidate = rng.nextLong(USER_ID_MIN, USER_ID_MAX + 1);
            if (userExists(candidate)) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty( );
    }


    private boolean userExists(long userId)
    {
        GetItemRequest request = GetItemRequest.builder( )
            .tableName(USER_TABLE_NAME)
            .key(Map.of("userId", AttributeValue.fromN(String.valueOf(userId))))
            .projectionExpression("userId")
            .build( );
        try {
            GetItemResponse response = client.getItem(request);
            return response.hasItem( ) && !response.item( ).isEmpty( );
        } catch (Exception e) {
            // Treat lookup failures as "not found" for retry purposes.
            // The feeder will see the eventual Optional.empty() and skip.
            log.warn("userId existence check failed for {}: {}", userId, e.getMessage( ));
            return false;
        }
    }

    // ------------------------------------------------------------------
    // Title derivation
    // ------------------------------------------------------------------


    /**
     * Derives a natural-language title from item attributes.
     * Example: "Women's 1970s Burberry Trench Coat in Good Condition"
     */
    static String deriveTitle(Brand brand, Product product, String gender,
        int year, String condition)
    {
        String genderWord = GENDER_WORDS.get(gender);
        String decade = (year / 10) * 10 + "s";
        String conditionWord = titleCaseCondition(condition);

        return String.format("%s %s %s %s in %s Condition",
            genderWord, decade, brand.name, product.titleNoun, conditionWord);
    }


    private static String titleCaseCondition(String condition)
    {
        StringBuilder sb = new StringBuilder(condition.length( ));
        boolean capitalizeNext = true;
        for (int i = 0; i < condition.length( ); i++) {
            char c = condition.charAt(i);
            if (c == ' ' || c == '/' || c == '-') {
                sb.append(c);
                capitalizeNext = true;
            } else if (capitalizeNext) {
                sb.append(Character.toUpperCase(c));
                capitalizeNext = false;
            } else {
                sb.append(c);
            }
        }
        return sb.toString( );
    }

    // ------------------------------------------------------------------
    // Price synthesis
    // ------------------------------------------------------------------


    private static BigDecimal synthesizePrice(Brand brand, Product product,
        int year, String condition,
        ThreadLocalRandom rng)
    {
        double base = rng.nextDouble(product.minPrice, product.maxPrice);
        base *= brand.priceMultiplier;
        int decadesOld = Math.max(0, (2025 - year) / 10);
        base *= (1.0 + Math.min(0.6, decadesOld * 0.08));
        double conditionFactor = switch (condition) {
            case "mint/unopened" -> 1.00;
            case "very good" -> 0.85;
            case "good" -> 0.65;
            case "fair" -> 0.40;
            case "poor" -> 0.20;
            default -> 0.65;
        };
        base *= conditionFactor;
        long dollars = Math.round(base);
        return new BigDecimal(dollars).subtract(new BigDecimal("0.01"))
            .setScale(2, RoundingMode.HALF_UP);
    }

    // ------------------------------------------------------------------
    // Gender and size selection
    // ------------------------------------------------------------------


    private static String pickGender(Brand brand, Product product, ThreadLocalRandom rng)
    {
        if (product.genderOverride != null) {
            return product.genderOverride;
        }
        double roll = rng.nextDouble( );
        double cum = 0;
        for (Map.Entry<String, Double> e : brand.genderMix.entrySet( )) {
            cum += e.getValue( );
            if (roll < cum) return e.getKey( );
        }
        return "U";
    }


    private static String pickSize(Product product, ThreadLocalRandom rng)
    {
        return switch (product.sizeScheme) {
            case WOMENS_NUMERIC -> String.valueOf(rng.nextInt(0, 9) * 2);
            case MENS_NUMERIC -> rng.nextInt(28, 45) + "x" + rng.nextInt(28, 37);
            case LETTER -> pick(LETTER_SIZES, rng);
            case SHOE_WOMENS -> String.valueOf(rng.nextDouble( ) < 0.5
                ? rng.nextInt(5, 11) : rng.nextInt(5, 11) + 0.5);
            case SHOE_MENS -> String.valueOf(rng.nextDouble( ) < 0.5
                ? rng.nextInt(7, 14) : rng.nextInt(7, 14) + 0.5);
            case ONE_SIZE -> "One Size";
            case NECK_SLEEVE -> rng.nextInt(14, 18) + "/" + rng.nextInt(32, 36);
        };
    }

    // ------------------------------------------------------------------
    // Utility
    // ------------------------------------------------------------------


    private static <T> T pick(List<T> list, Random rng)
    {
        return list.get(rng.nextInt(list.size( )));
    }

    // ------------------------------------------------------------------
    // Brand, product, and enumeration definitions
    // ------------------------------------------------------------------

    private enum SizeScheme
    {
        WOMENS_NUMERIC, MENS_NUMERIC, LETTER, SHOE_WOMENS, SHOE_MENS,
        ONE_SIZE, NECK_SLEEVE
    }

    private static final class Product
    {
        final String category;
        final String subcategory;
        final String style;
        final String titleNoun;
        final SizeScheme sizeScheme;
        final double minPrice;
        final double maxPrice;
        final String genderOverride;


        Product(String category, String subcategory, String style, String titleNoun,
            SizeScheme sizeScheme, double minPrice, double maxPrice,
            String genderOverride)
        {
            this.category = category;
            this.subcategory = subcategory;
            this.style = style;
            this.titleNoun = titleNoun;
            this.sizeScheme = sizeScheme;
            this.minPrice = minPrice;
            this.maxPrice = maxPrice;
            this.genderOverride = genderOverride;
        }
    }

    private static final class Brand
    {
        final String name;
        final int earliestYear;
        final int latestYear;
        final double priceMultiplier;
        final List<String> colors;
        final List<Product> products;
        final Map<String, Double> genderMix;


        Brand(String name, int earliestYear, int latestYear, double priceMultiplier,
            List<String> colors, List<Product> products,
            Map<String, Double> genderMix)
        {
            this.name = name;
            this.earliestYear = earliestYear;
            this.latestYear = latestYear;
            this.priceMultiplier = priceMultiplier;
            this.colors = colors;
            this.products = products;
            this.genderMix = genderMix;
        }
    }

    // ---- Shared product catalog pieces ----

    private static final Product TRENCH_COAT = new Product(
        "outerwear", "coats", "trench", "Trench Coat",
        SizeScheme.WOMENS_NUMERIC, 600, 1800, null);
    private static final Product PEACOAT = new Product(
        "outerwear", "coats", "peacoat", "Peacoat",
        SizeScheme.LETTER, 400, 900, null);
    private static final Product OVERCOAT = new Product(
        "outerwear", "coats", "overcoat", "Overcoat",
        SizeScheme.LETTER, 500, 1400, null);
    private static final Product LEATHER_JACKET = new Product(
        "outerwear", "jackets", "leather", "Leather Jacket",
        SizeScheme.LETTER, 500, 1600, null);
    private static final Product BLAZER = new Product(
        "outerwear", "jackets", "blazer", "Blazer",
        SizeScheme.LETTER, 300, 900, null);

    private static final Product OXFORD_SHIRT = new Product(
        "tops", "shirts", "oxford", "Oxford Shirt",
        SizeScheme.NECK_SLEEVE, 80, 240, null);
    private static final Product DRESS_SHIRT = new Product(
        "tops", "shirts", "dress", "Dress Shirt",
        SizeScheme.NECK_SLEEVE, 100, 300, null);
    private static final Product POLO_SHIRT = new Product(
        "tops", "shirts", "polo", "Polo Shirt",
        SizeScheme.LETTER, 60, 180, null);
    private static final Product BLOUSE = new Product(
        "tops", "shirts", "blouse", "Silk Blouse",
        SizeScheme.WOMENS_NUMERIC, 150, 600, "W");
    private static final Product SWEATER = new Product(
        "tops", "knitwear", "sweater", "Cashmere Sweater",
        SizeScheme.LETTER, 180, 700, null);

    private static final Product PENCIL_SKIRT = new Product(
        "bottoms", "skirts", "pencil", "Pencil Skirt",
        SizeScheme.WOMENS_NUMERIC, 120, 500, "W");
    private static final Product APRES_SKIRT = new Product(
        "bottoms", "skirts", "a-line", "A-Line Skirt",
        SizeScheme.WOMENS_NUMERIC, 100, 400, "W");

    private static final Product DAY_DRESS = new Product(
        "dresses", "dresses", "day", "Day Dress",
        SizeScheme.WOMENS_NUMERIC, 200, 800, "W");
    private static final Product COCKTAIL_DRESS = new Product(
        "dresses", "dresses", "cocktail", "Cocktail Dress",
        SizeScheme.WOMENS_NUMERIC, 300, 1400, "W");
    private static final Product EVENING_GOWN = new Product(
        "dresses", "dresses", "evening", "Evening Gown",
        SizeScheme.WOMENS_NUMERIC, 500, 2500, "W");

    private static final Product OXFORD_SHOE = new Product(
        "shoes", "shoes", "oxford", "Oxford Shoes",
        SizeScheme.SHOE_MENS, 200, 800, null);
    private static final Product LOAFER = new Product(
        "shoes", "shoes", "loafer", "Loafers",
        SizeScheme.SHOE_MENS, 180, 700, null);
    private static final Product PUMP = new Product(
        "shoes", "shoes", "pump", "Pumps",
        SizeScheme.SHOE_WOMENS, 200, 900, "W");

    private static final Product HANDBAG = new Product(
        "accessories", "bags", "handbag", "Handbag",
        SizeScheme.ONE_SIZE, 400, 3500, "W");
    private static final Product BRIEFCASE = new Product(
        "accessories", "bags", "briefcase", "Leather Briefcase",
        SizeScheme.ONE_SIZE, 500, 2800, null);
    private static final Product COURIER_BAG = new Product(
        "accessories", "bags", "courier", "Courier Bag",
        SizeScheme.ONE_SIZE, 300, 1200, null);

    private static final Product BELT = new Product(
        "accessories", "small-leather", "belt", "Leather Belt",
        SizeScheme.LETTER, 120, 600, null);
    private static final Product NECKTIE = new Product(
        "accessories", "neckwear", "necktie", "Silk Necktie",
        SizeScheme.ONE_SIZE, 60, 280, "M");
    private static final Product SCARF = new Product(
        "accessories", "neckwear", "scarf", "Silk Scarf",
        SizeScheme.ONE_SIZE, 150, 800, null);

    // ---- Brand roster ----

    private static final List<Brand> BRANDS = List.of(
        new Brand("Burberry", 1960, 1995, 1.3,
            List.of("Mistral Sage", "Honey", "Stone", "Camel", "Thistle",
                "Carbon Navy", "Dark Truffle", "Oxblood", "Heather Grey",
                "Moss Green", "Ink Blue"),
            List.of(TRENCH_COAT, OVERCOAT, BLAZER, BLOUSE, SCARF, NECKTIE, BELT),
            Map.of("W", 0.55, "M", 0.40, "U", 0.05)),

        new Brand("Brooks Brothers", 1955, 1995, 0.9,
            List.of("Navy", "Charcoal", "Heather Grey", "Cream", "British Tan",
                "Forest Green", "Burgundy", "White", "Ecru", "Stone"),
            List.of(OVERCOAT, PEACOAT, BLAZER, OXFORD_SHIRT, DRESS_SHIRT,
                POLO_SHIRT, SWEATER, NECKTIE, BELT, OXFORD_SHOE, LOAFER),
            Map.of("M", 0.75, "W", 0.20, "U", 0.05)),

        new Brand("Coach", 1965, 1995, 1.1,
            List.of("British Tan", "Saddle", "Cordovan", "Black", "Mahogany",
                "Chestnut", "Walnut", "Oxblood", "Natural"),
            List.of(HANDBAG, BRIEFCASE, COURIER_BAG, BELT),
            Map.of("W", 0.55, "M", 0.35, "U", 0.10)),

        new Brand("Hermès", 1960, 1995, 2.5,
            List.of("Barenia", "Orange H", "Gold", "Etoupe", "Rouge H",
                "Bleu de Prusse", "Noir", "Fauve", "Vert Anglais",
                "Craie", "Rose Tyrien"),
            List.of(HANDBAG, BELT, NECKTIE, SCARF, BLAZER),
            Map.of("W", 0.50, "M", 0.40, "U", 0.10)),

        new Brand("Ralph Lauren", 1970, 1995, 1.0,
            List.of("Polo Navy", "Cream", "British Tan", "Hunter Green",
                "Burgundy", "Oxford Blue", "White", "Khaki", "Oxblood"),
            List.of(BLAZER, OXFORD_SHIRT, DRESS_SHIRT, POLO_SHIRT, SWEATER,
                PENCIL_SKIRT, DAY_DRESS, NECKTIE, BELT, LOAFER),
            Map.of("M", 0.55, "W", 0.40, "U", 0.05)),

        new Brand("Yves Saint Laurent", 1962, 1995, 1.8,
            List.of("Noir", "Ivoire", "Rouge", "Bleu Marine", "Emeraude",
                "Or", "Violet", "Fuchsia", "Safran"),
            List.of(BLAZER, BLOUSE, PENCIL_SKIRT, DAY_DRESS, COCKTAIL_DRESS,
                EVENING_GOWN, PUMP, SCARF),
            Map.of("W", 0.85, "M", 0.10, "U", 0.05)),

        new Brand("Chanel", 1960, 1995, 2.2,
            List.of("Noir", "Ivoire", "Beige Clair", "Rose Poudré",
                "Rouge Cardinal", "Navy", "Ecru", "Tweed Grey", "Camel"),
            List.of(BLAZER, BLOUSE, PENCIL_SKIRT, DAY_DRESS, COCKTAIL_DRESS,
                HANDBAG, PUMP, SCARF),
            Map.of("W", 0.95, "U", 0.05)),

        new Brand("Gucci", 1965, 1995, 1.7,
            List.of("Nero", "Cuoio", "Rosso", "Verde Bosco", "Blu Notte",
                "Oro", "Bianco Sporco", "Cognac", "Bordeaux"),
            List.of(HANDBAG, BRIEFCASE, LOAFER, BELT, NECKTIE, SCARF, BLAZER),
            Map.of("W", 0.45, "M", 0.50, "U", 0.05)),

        new Brand("Givenchy", 1960, 1995, 1.6,
            List.of("Noir", "Blanc Cassé", "Rouge Vif", "Marine",
                "Gris Perle", "Vert Amande", "Dorée"),
            List.of(BLAZER, BLOUSE, DAY_DRESS, COCKTAIL_DRESS, EVENING_GOWN,
                PUMP, SCARF),
            Map.of("W", 0.90, "U", 0.10)),

        new Brand("Pendleton", 1960, 1995, 0.6,
            List.of("Red Buffalo Check", "Black Watch Tartan", "Camel",
                "Hunter Green", "Navy Plaid", "Charcoal", "Cream",
                "Forest Plaid", "Oxblood Tartan"),
            List.of(PEACOAT, OVERCOAT, BLAZER, SWEATER, SCARF),
            Map.of("M", 0.55, "W", 0.35, "U", 0.10)),

        new Brand("Pucci", 1960, 1985, 1.4,
            List.of("Capri Print", "Palio Print", "Vivara Swirl",
                "Battistero Print", "Turquoise", "Fuchsia", "Citron",
                "Coral", "Emerald Print"),
            List.of(BLOUSE, PENCIL_SKIRT, APRES_SKIRT, DAY_DRESS,
                COCKTAIL_DRESS, SCARF),
            Map.of("W", 0.95, "U", 0.05)),

        new Brand("Oscar de la Renta", 1965, 1995, 1.5,
            List.of("Ivory", "Blush", "Emerald", "Garnet", "Midnight",
                "Champagne", "Rose Gold", "Onyx"),
            List.of(BLOUSE, DAY_DRESS, COCKTAIL_DRESS, EVENING_GOWN, PUMP),
            Map.of("W", 1.0)),

        new Brand("Loro Piana", 1970, 1995, 1.9,
            List.of("Vicuña", "Cammello", "Ghiaccio", "Grigio Pietra",
                "Blu Inchiostro", "Nocciola", "Panna"),
            List.of(OVERCOAT, BLAZER, SWEATER, SCARF),
            Map.of("M", 0.55, "W", 0.40, "U", 0.05)),

        new Brand("Pierre Cardin", 1962, 1985, 1.1,
            List.of("Electric Blue", "Chartreuse", "Vermillion", "Noir",
                "Blanc", "Silver", "Bronze", "Persimmon"),
            List.of(BLAZER, BLOUSE, DAY_DRESS, COCKTAIL_DRESS, PENCIL_SKIRT,
                NECKTIE),
            Map.of("W", 0.60, "M", 0.35, "U", 0.05))
    );

    // ---- Enumerations ----

    private static final List<String> CONDITIONS = List.of(
        "mint/unopened", "very good", "good", "fair", "poor");

    private static final List<String> LETTER_SIZES = List.of(
        "XS", "S", "M", "L", "XL", "XXL");

    private static final Map<String, String> GENDER_WORDS = Map.of(
        "M", "Men's",
        "W", "Women's",
        "U", "Unisex");
}
