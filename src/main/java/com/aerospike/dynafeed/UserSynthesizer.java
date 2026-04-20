package com.aerospike.dynafeed;


import com.aerospike.dynafeed.User;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Synthesizes plausible User records with names reflecting the diversity of
 * the contemporary United States and addresses that follow USPS conventions.
 *
 * <p>Design notes:
 * <ul>
 *   <li>First and last names are drawn from independently shuffled pools
 *       representing major American heritage groups — European, African
 *       American, Hispanic, East Asian, South Asian, Middle Eastern, and
 *       others. Names are paired independently, so "Lakshmi O'Brien" or
 *       "Hiroshi Martinez" can occur, reflecting the reality of a country
 *       where naming and ancestry don't always align.</li>
 *   <li>Addresses are assembled from street-number, street-name, and
 *       street-type pools, paired with a city/state/ZIP triple drawn as a
 *       unit from a curated list of real US cities. This avoids nonsense
 *       like "Boston, FL 94043".</li>
 *   <li>Phone numbers use the 555-01xx reservation (NANPA-reserved for
 *       fictional use) to guarantee the generated numbers never route to
 *       a real person.</li>
 *   <li>Area codes are drawn from the area codes actually assigned to the
 *       synthesized city's state, keeping the phone number at least
 *       loosely consistent with the address.</li>
 * </ul>
 */
public final class UserSynthesizer
{
    private final AtomicLong idSequence;


    public UserSynthesizer(long startingId)
    {
        this.idSequence = new AtomicLong(startingId);
    }


    public UserSynthesizer( )
    {
        this(1L);
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------


    public User synthesize( )
    {
        ThreadLocalRandom rng = ThreadLocalRandom.current( );

        String firstName = pick(FIRST_NAMES, rng);
        String lastName = pick(LAST_NAMES, rng);
        CityStateZip location = pick(LOCATIONS, rng);

        User user = new User( );
        user.setUserId(idSequence.getAndIncrement( ));
        user.setFirstName(firstName);
        user.setLastName(lastName);
        user.setEmail(synthesizeEmail(firstName, lastName, rng));
        user.setPhoneNumber(synthesizePhone(location.state, rng));
        user.setAddressLine1(synthesizeAddressLine1(rng));
        user.setAddressLine2(synthesizeAddressLine2(rng)); // may be null
        user.setCity(location.city);
        user.setState(location.state);
        user.setZip(location.zip);
        return user;
    }

    // ------------------------------------------------------------------
    // Email / phone / address synthesis
    // ------------------------------------------------------------------


    private static String synthesizeEmail(String firstName, String lastName,
        ThreadLocalRandom rng)
    {
        String domain = pick(EMAIL_DOMAINS, rng);
        // Normalize names: strip apostrophes, spaces, diacritics-bearing
        // characters we generate (we don't actually emit any), and lowercase.
        String fn = firstName.toLowerCase( ).replaceAll("[^a-z]", "");
        String ln = lastName.toLowerCase( ).replaceAll("[^a-z]", "");
        int pattern = rng.nextInt(4);
        return switch (pattern) {
            case 0 -> fn + "." + ln + "@" + domain;
            case 1 -> fn + ln + rng.nextInt(100) + "@" + domain;
            case 2 -> fn.charAt(0) + ln + "@" + domain;
            default -> fn + "_" + ln + "@" + domain;
        };
    }


    /**
     * Produces a phone number in the format "+1-XXX-555-01YY" using a real
     * area code for the user's state and the 555-01xx reservation block,
     * which the NANPA holds aside for fictional use.
     */
    private static String synthesizePhone(String state, ThreadLocalRandom rng)
    {
        List<String> areaCodes = AREA_CODES_BY_STATE.getOrDefault(state, DEFAULT_AREA_CODES);
        String areaCode = pick(areaCodes, rng);
        int lastTwo = rng.nextInt(0, 100);
        return String.format("+1-%s-555-01%02d", areaCode, lastTwo);
    }


    private static String synthesizeAddressLine1(ThreadLocalRandom rng)
    {
        int number = rng.nextInt(1, 9999);
        String streetName = pick(STREET_NAMES, rng);
        String streetType = pick(STREET_TYPES, rng);
        return number + " " + streetName + " " + streetType;
    }


    /**
     * Returns null 70% of the time (most addresses have no line 2) and
     * an apartment/suite/unit designator the rest of the time.
     */
    private static String synthesizeAddressLine2(ThreadLocalRandom rng)
    {
        if (rng.nextDouble( ) < 0.70) {
            return null;
        }
        String prefix = pick(LINE2_PREFIXES, rng);
        // Unit identifiers are a mix of numeric, alphanumeric, and letter-only.
        int form = rng.nextInt(3);
        String unit = switch (form) {
            case 0 -> String.valueOf(rng.nextInt(1, 500));
            case 1 -> rng.nextInt(1, 50) + String.valueOf((char) ('A' + rng.nextInt(26)));
            default -> String.valueOf((char) ('A' + rng.nextInt(26)));
        };
        return prefix + " " + unit;
    }

    // ------------------------------------------------------------------
    // Utility
    // ------------------------------------------------------------------


    private static <T> T pick(List<T> list, Random rng)
    {
        return list.get(rng.nextInt(list.size( )));
    }

    // ------------------------------------------------------------------
    // Name pools
    // ------------------------------------------------------------------
    // The pools below are deliberately assembled across heritage groups.
    // Pairing is independent, reflecting the reality that in the US, a
    // first name and last name don't reliably co-predict ancestry.

    private static final List<String> FIRST_NAMES = List.of(
        // European-descended (English, Irish, Italian, German, Slavic, Jewish, etc.)
        "Jack", "Emma", "William", "Olivia", "James", "Sophia", "Benjamin",
        "Charlotte", "Henry", "Amelia", "Liam", "Ava", "Ethan", "Isabella",
        "Connor", "Maeve", "Declan", "Siobhan", "Giuseppe", "Francesca",
        "Dmitri", "Anastasia", "Hans", "Greta", "Aaron", "Rachel", "David",
        "Sarah", "Samuel", "Rebecca", "Joshua", "Hannah",

        // African American
        "Jamal", "Aaliyah", "Marcus", "Zora", "Darius", "Imani", "Terrell",
        "Destiny", "Malik", "Jada", "DeShawn", "Ebony", "Xavier", "Amara",
        "Tyrese", "Nia", "Kendrick", "Tanisha", "Deondre", "Keisha",

        // Hispanic (Mexican, Puerto Rican, Cuban, Dominican, Central/South American)
        "Miguel", "Sofia", "Diego", "Camila", "Mateo", "Valentina", "Luis",
        "Isabella", "Carlos", "Lucia", "Javier", "Elena", "Ricardo", "Gabriela",
        "Alejandro", "Mariana", "Rafael", "Ximena", "Andrés", "Paloma",

        // East Asian (Chinese, Japanese, Korean, Vietnamese)
        "Wei", "Mei", "Hiroshi", "Yuki", "Jun", "Haruna", "Minjun", "Seoyeon",
        "Thanh", "Linh", "Jianyu", "Xiuying", "Takeshi", "Sakura", "Donghae",
        "Jiwoo", "Duy", "Huong",

        // South Asian (Indian, Pakistani, Bangladeshi, Sri Lankan)
        "Lakshmi", "Arjun", "Priya", "Rohan", "Aanya", "Vikram", "Kavya",
        "Siddharth", "Ananya", "Rahul", "Deepika", "Rajesh", "Neha", "Amit",
        "Shreya", "Imran", "Fatima", "Zainab", "Tariq",

        // Middle Eastern / North African (Arab, Persian, Turkish, Israeli)
        "Omar", "Layla", "Yousef", "Aisha", "Karim", "Noor", "Farid", "Zahra",
        "Hassan", "Leila", "Reza", "Shirin", "Mehmet", "Aylin", "Eitan", "Yael",

        // Other (Filipino, Pacific Islander, Native American, Ethiopian/Eritrean, etc.)
        "Kai", "Leilani", "Tala", "Joaquin", "Makoa", "Kalani", "Abebe",
        "Selam", "Dakota", "Winona");

    private static final List<String> LAST_NAMES = List.of(
        // European-descended
        "Smith", "Johnson", "Brown", "Miller", "Davis", "Wilson", "Anderson",
        "Taylor", "Thomas", "Moore", "Jackson", "Martin", "Lee", "Thompson",
        "O'Brien", "O'Connor", "Murphy", "Kelly", "Rossi", "Romano", "Ferrari",
        "Bianchi", "Kowalski", "Nowak", "Schmidt", "Müller", "Weber",
        "Cohen", "Goldberg", "Levine", "Shapiro", "Feinstein",

        // African American (includes historically Black surnames and common surnames)
        "Washington", "Jefferson", "Freeman", "Booker", "Tubman", "DuBois",
        "Carver", "Robinson", "Harris", "Jenkins", "Mosley", "Dawson",

        // Hispanic
        "García", "Rodríguez", "Martínez", "Hernández", "López", "González",
        "Pérez", "Sánchez", "Ramírez", "Torres", "Flores", "Rivera", "Gómez",
        "Díaz", "Reyes", "Morales", "Ortiz", "Gutiérrez", "Ruiz", "Castillo",
        "Vargas", "Mendoza",

        // East Asian
        "Wang", "Li", "Zhang", "Liu", "Chen", "Yang", "Huang", "Zhao",
        "Tanaka", "Suzuki", "Takahashi", "Watanabe", "Yamamoto", "Kim",
        "Park", "Choi", "Jung", "Nguyen", "Tran", "Pham", "Vo", "Bui",

        // South Asian
        "Singh", "Patel", "Sharma", "Kumar", "Gupta", "Shah", "Rao", "Reddy",
        "Desai", "Mehta", "Iyer", "Nair", "Bhatt", "Chaudhary", "Kapoor",
        "Khan", "Ahmed", "Rahman", "Hossain",

        // Middle Eastern / North African
        "Al-Hassan", "Haddad", "Nasser", "Khoury", "Mansour", "Saleh",
        "Karimi", "Hosseini", "Farsi", "Yılmaz", "Demir", "Cohen", "Levi",
        "Mizrahi",

        // Other
        "Reyes", "Santos", "Delacruz", "Aquino", "Makoa", "Kahale",
        "Tekle", "Gebremariam", "Littlefoot", "Redcloud");

    // ------------------------------------------------------------------
    // Address pools
    // ------------------------------------------------------------------

    private static final List<String> STREET_NAMES = List.of(
        "Main", "Oak", "Elm", "Maple", "Cedar", "Pine", "Washington",
        "Lincoln", "Jefferson", "Madison", "Park", "Lake", "Hill", "River",
        "Sunset", "Highland", "Willow", "Chestnut", "Walnut", "Birch",
        "Mission", "Valencia", "Market", "Broadway", "State", "Church",
        "Franklin", "Jackson", "Adams", "Monroe", "Roosevelt", "Kennedy",
        "Martin Luther King", "Cesar Chavez", "Magnolia", "Dogwood",
        "Juniper", "Aspen", "Sequoia", "Cypress", "Garfield", "Taylor",
        "Polk", "Fillmore", "Buchanan", "Webster", "Larkin", "Hyde");

    private static final List<String> STREET_TYPES = List.of(
        "Street", "Avenue", "Boulevard", "Drive", "Lane", "Road", "Court",
        "Place", "Way", "Terrace", "Circle", "Parkway");

    private static final List<String> LINE2_PREFIXES = List.of(
        "Apt", "Suite", "Unit", "#");

    private static final List<String> EMAIL_DOMAINS = List.of(
        "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com",
        "proton.me", "aol.com", "me.com");

    // ------------------------------------------------------------------
    // Location data
    // ------------------------------------------------------------------

    /**
     * A plausible US city/state/ZIP triple. ZIP is a real ZIP in the named
     * city (not the only one, but a real one), keeping the record
     * internally consistent.
     */
    private record CityStateZip(String city, String state, String zip)
    {
    }

    private static final List<CityStateZip> LOCATIONS = List.of(
        // California
        new CityStateZip("San Francisco", "CA", "94110"),
        new CityStateZip("Los Angeles", "CA", "90028"),
        new CityStateZip("San Diego", "CA", "92101"),
        new CityStateZip("Oakland", "CA", "94612"),
        new CityStateZip("San Jose", "CA", "95113"),
        new CityStateZip("Sacramento", "CA", "95814"),
        // New York
        new CityStateZip("New York", "NY", "10011"),
        new CityStateZip("Brooklyn", "NY", "11201"),
        new CityStateZip("Queens", "NY", "11354"),
        new CityStateZip("Buffalo", "NY", "14202"),
        new CityStateZip("Rochester", "NY", "14604"),
        // Texas
        new CityStateZip("Houston", "TX", "77002"),
        new CityStateZip("Austin", "TX", "78701"),
        new CityStateZip("Dallas", "TX", "75201"),
        new CityStateZip("San Antonio", "TX", "78205"),
        new CityStateZip("El Paso", "TX", "79901"),
        // Florida
        new CityStateZip("Miami", "FL", "33130"),
        new CityStateZip("Orlando", "FL", "32801"),
        new CityStateZip("Tampa", "FL", "33602"),
        new CityStateZip("Jacksonville", "FL", "32202"),
        // Illinois
        new CityStateZip("Chicago", "IL", "60614"),
        new CityStateZip("Evanston", "IL", "60201"),
        // Massachusetts (leading-zero ZIP, which is why we store as String)
        new CityStateZip("Boston", "MA", "02116"),
        new CityStateZip("Cambridge", "MA", "02139"),
        new CityStateZip("Worcester", "MA", "01608"),
        // Washington
        new CityStateZip("Seattle", "WA", "98101"),
        new CityStateZip("Tacoma", "WA", "98402"),
        new CityStateZip("Spokane", "WA", "99201"),
        // Oregon
        new CityStateZip("Portland", "OR", "97205"),
        new CityStateZip("Eugene", "OR", "97401"),
        // Colorado
        new CityStateZip("Denver", "CO", "80202"),
        new CityStateZip("Boulder", "CO", "80302"),
        // Georgia
        new CityStateZip("Atlanta", "GA", "30303"),
        new CityStateZip("Savannah", "GA", "31401"),
        // North Carolina
        new CityStateZip("Charlotte", "NC", "28202"),
        new CityStateZip("Raleigh", "NC", "27601"),
        new CityStateZip("Durham", "NC", "27701"),
        // Pennsylvania
        new CityStateZip("Philadelphia", "PA", "19103"),
        new CityStateZip("Pittsburgh", "PA", "15222"),
        // Arizona
        new CityStateZip("Phoenix", "AZ", "85003"),
        new CityStateZip("Tucson", "AZ", "85701"),
        // Nevada
        new CityStateZip("Las Vegas", "NV", "89101"),
        new CityStateZip("Reno", "NV", "89501"),
        // Michigan
        new CityStateZip("Detroit", "MI", "48226"),
        new CityStateZip("Ann Arbor", "MI", "48104"),
        // Ohio
        new CityStateZip("Cleveland", "OH", "44113"),
        new CityStateZip("Columbus", "OH", "43215"),
        // Minnesota
        new CityStateZip("Minneapolis", "MN", "55401"),
        new CityStateZip("Saint Paul", "MN", "55102"),
        // DC / Virginia / Maryland
        new CityStateZip("Washington", "DC", "20001"),
        new CityStateZip("Arlington", "VA", "22201"),
        new CityStateZip("Baltimore", "MD", "21202"),
        // New Jersey
        new CityStateZip("Jersey City", "NJ", "07302"),
        new CityStateZip("Newark", "NJ", "07102"),
        // Hawaii
        new CityStateZip("Honolulu", "HI", "96813"),
        // Louisiana
        new CityStateZip("New Orleans", "LA", "70112"),
        // Tennessee
        new CityStateZip("Nashville", "TN", "37203"),
        new CityStateZip("Memphis", "TN", "38103"));

    /**
     * Area codes actually assigned to each state. Not exhaustive — just a
     * representative sample per state so the generated phone number's area
     * code is geographically plausible relative to the address.
     */
    private static final java.util.Map<String, List<String>> AREA_CODES_BY_STATE =
        java.util.Map.ofEntries(
            java.util.Map.entry("CA", List.of("415", "510", "650", "408", "213", "310", "619", "916", "707")),
            java.util.Map.entry("NY", List.of("212", "718", "646", "347", "917", "716", "585")),
            java.util.Map.entry("TX", List.of("713", "512", "214", "210", "915", "832", "469")),
            java.util.Map.entry("FL", List.of("305", "407", "813", "904", "954", "786")),
            java.util.Map.entry("IL", List.of("312", "773", "847", "630")),
            java.util.Map.entry("MA", List.of("617", "508", "781", "978")),
            java.util.Map.entry("WA", List.of("206", "253", "509", "425")),
            java.util.Map.entry("OR", List.of("503", "541", "971")),
            java.util.Map.entry("CO", List.of("303", "720", "970")),
            java.util.Map.entry("GA", List.of("404", "678", "912", "770")),
            java.util.Map.entry("NC", List.of("704", "919", "336", "984")),
            java.util.Map.entry("PA", List.of("215", "412", "717", "610")),
            java.util.Map.entry("AZ", List.of("602", "520", "480", "623")),
            java.util.Map.entry("NV", List.of("702", "775")),
            java.util.Map.entry("MI", List.of("313", "734", "248", "616")),
            java.util.Map.entry("OH", List.of("216", "614", "330", "513")),
            java.util.Map.entry("MN", List.of("612", "651", "952", "763")),
            java.util.Map.entry("DC", List.of("202")),
            java.util.Map.entry("VA", List.of("703", "571", "804")),
            java.util.Map.entry("MD", List.of("410", "443", "301")),
            java.util.Map.entry("NJ", List.of("201", "973", "732", "609")),
            java.util.Map.entry("HI", List.of("808")),
            java.util.Map.entry("LA", List.of("504", "225", "337")),
            java.util.Map.entry("TN", List.of("615", "901", "423")));

    private static final List<String> DEFAULT_AREA_CODES = List.of("202", "212", "415");
}
