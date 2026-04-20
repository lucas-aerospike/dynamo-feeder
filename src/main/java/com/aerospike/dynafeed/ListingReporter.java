package com.aerospike.dynafeed;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Produces continuous human-readable reports on individual users' listing
 * activity. Designed to run as an independent process — possibly on its own
 * lightweight AWS instance — to add read load against the {@code inventory}
 * table's {@code listingUser-index} GSI as well as the {@code user} table.
 *
 * <p>Each report cycle:
 * <ol>
 *   <li>guesses a random userId in the same {@value #USER_ID_MIN}..{@value #USER_ID_MAX}
 *       range used by {@link com.aerospike.dynafeed.ItemSynthesizer},
 *       up to {@value #MAX_USER_LOOKUP_ATTEMPTS} attempts;</li>
 *   <li>fetches the {@link User} record (full attribute set, used for the
 *       contact-information header);</li>
 *   <li>queries the {@code listingUser-index} GSI for all items listed by
 *       that user (returning {@code itemId}, {@code title}, {@code price});</li>
 *   <li>prints a formatted report to stdout.</li>
 * </ol>
 *
 * <p>Reports are written via {@link System#out} rather than the SLF4J logger
 * because they are user-facing output, not operational diagnostics. Logger
 * output continues to flow through the standard logging pipeline.
 *
 * <p>This class has its own {@link #main(String[])} so it can be invoked
 * directly from the assembled JAR alongside the user/item feeders, e.g.
 * {@code java -cp dynafeed.jar com.aerospike.dynafeed.ListingReporter}.
 *
 * <h2>Required GSI</h2>
 * <p>Before this class will function, the {@code inventory} table must have
 * a GSI named {@value #LISTING_USER_INDEX_NAME} with {@code listingUser} as
 * the partition key, projecting at minimum {@code title} and {@code price}
 * (in addition to the automatically-included base table partition key
 * {@code itemId}). Create this in the AWS console.
 */
public final class ListingReporter
{
    private static final Logger log = LoggerFactory.getLogger(ListingReporter.class);

    private static final int DEFAULT_THREADS = Runtime.getRuntime( ).availableProcessors( );
    private static final Duration DEFAULT_DELAY = Duration.ofSeconds(5);
    private static final long USER_ID_MIN = 1L;
    private static final long USER_ID_MAX = 6_000L;
    private static final int MAX_USER_LOOKUP_ATTEMPTS = 10;

    private static final String USER_TABLE_NAME = "user";
    private static final String INVENTORY_TABLE_NAME = "inventory";
    private static final String LISTING_USER_INDEX_NAME = "listingUser-index";

    private final int threadCount;
    private final Duration delay;
    private final DynamoDbClient lowLevelClient;
    private final DynamoDbTable<User> userTable;
    private final DynamoDbTable<InventoryItem> inventoryTable;
    private final DynamoDbIndex<InventoryItem> listingUserIndex;

    private final AtomicLong reportCount = new AtomicLong( );
    private final AtomicLong skippedCount = new AtomicLong( );
    private final AtomicLong failureCount = new AtomicLong( );
    private volatile ExecutorService executor;
    private volatile boolean running;


    public ListingReporter(DynamoDbClient lowLevelClient,
        DynamoDbEnhancedClient enhancedClient,
        int threadCount,
        Duration delay)
    {
        if (threadCount < 1) {
            throw new IllegalArgumentException("threadCount must be >= 1");
        }
        if (delay == null || delay.isNegative( )) {
            throw new IllegalArgumentException("delay must be non-null and non-negative");
        }
        this.threadCount = threadCount;
        this.delay = delay;
        this.lowLevelClient = lowLevelClient;
        this.userTable = enhancedClient.table(USER_TABLE_NAME, TableSchema.fromBean(User.class));
        this.inventoryTable = enhancedClient.table(INVENTORY_TABLE_NAME,
            TableSchema.fromBean(InventoryItem.class));
        this.listingUserIndex = inventoryTable.index(LISTING_USER_INDEX_NAME);
    }


    public ListingReporter(DynamoDbClient lowLevelClient, DynamoDbEnhancedClient enhancedClient)
    {
        this(lowLevelClient, enhancedClient, DEFAULT_THREADS, DEFAULT_DELAY);
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------


    public synchronized void start( )
    {
        if (running) {
            log.warn("ListingReporter already running; ignoring start()");
            return;
        }
        log.info("Starting ListingReporter: threads={}, delay={}", threadCount, delay);
        AtomicLong threadOrdinal = new AtomicLong( );
        executor = Executors.newFixedThreadPool(threadCount, r -> {
            Thread t = new Thread(r, "listing-reporter-" + threadOrdinal.getAndIncrement( ));
            t.setDaemon(true);
            return t;
        });
        running = true;
        for (int i = 0; i < threadCount; i++) {
            executor.submit(new ReporterTask( ));
        }
    }


    public synchronized void stop( )
    {
        if (!running) {
            return;
        }
        log.info("Stopping ListingReporter: reports={}, skipped={}, failures={}",
            reportCount.get( ), skippedCount.get( ), failureCount.get( ));
        running = false;
        executor.shutdown( );
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("ListingReporter workers did not terminate within 10s; forcing shutdown");
                executor.shutdownNow( );
            }
        } catch (InterruptedException e) {
            Thread.currentThread( ).interrupt( );
            executor.shutdownNow( );
        }
    }


    public long getReportCount( )
    {
        return reportCount.get( );
    }


    public long getSkippedCount( )
    {
        return skippedCount.get( );
    }


    public long getFailureCount( )
    {
        return failureCount.get( );
    }

    // ------------------------------------------------------------------
    // main
    // ------------------------------------------------------------------


    public static void main(String[] args) throws InterruptedException
    {
        var clientDriver = DynamoDbClient.builder()
            .region(Region.US_WEST_2)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build( );

        var enhancedClient = DynamoDbEnhancedClient.builder( )
            .dynamoDbClient(clientDriver)
            .build( );

        var reporter = new ListingReporter(clientDriver, enhancedClient);

        Runtime.getRuntime( ).addShutdownHook(new Thread(( ) -> {
            reporter.stop( );
            clientDriver.close( );
        }));

        reporter.start( );

        Thread.currentThread( ).join( );
    }

    // ------------------------------------------------------------------
    // Worker
    // ------------------------------------------------------------------


    private final class ReporterTask implements Runnable
    {

        @Override
        public void run( )
        {
            log.debug("ReporterTask started on {}", Thread.currentThread( ).getName( ));
            while (running) {
                runOnce( );
                if (!sleepQuietly(delay)) {
                    break;
                }
            }
            log.debug("ReporterTask exiting on {}", Thread.currentThread( ).getName( ));
        }


        private void runOnce( )
        {
            ThreadLocalRandom rng = ThreadLocalRandom.current( );

            Optional<Long> chosenId = findExistingUserId(rng);
            if (chosenId.isEmpty( )) {
                skippedCount.incrementAndGet( );
                log.info("Skipped report cycle: no valid userId found in {} attempts",
                    MAX_USER_LOOKUP_ATTEMPTS);
                return;
            }

            long userId = chosenId.get( );

            try {
                User user = userTable.getItem(Key.builder( ).partitionValue(userId).build( ));
                if (user == null) {
                    // Race: existed during the lookup, gone now. Treat as skip.
                    skippedCount.incrementAndGet( );
                    return;
                }

                List<InventoryItem> listings = queryListings(userId);
                printReport(user, listings);
                reportCount.incrementAndGet( );
            } catch (Exception e) {
                failureCount.incrementAndGet( );
                log.warn("Report cycle failed for userId={}: {}", userId, e.getMessage( ));
            }
        }


        private List<InventoryItem> queryListings(long userId)
        {
            QueryConditional condition = QueryConditional.keyEqualTo(
                Key.builder( ).partitionValue(userId).build( ));
            QueryEnhancedRequest request = QueryEnhancedRequest.builder( )
                .queryConditional(condition)
                .build( );

            // GSI queries return pages; flatten across all pages. At our
            // scale a user will have at most a few dozen listings, so the
            // cost of materializing the full list is negligible.
            List<InventoryItem> all = new ArrayList<>( );
            listingUserIndex.query(request).forEach(page -> all.addAll(page.items( )));
            return all;
        }


        private boolean sleepQuietly(Duration d)
        {
            try {
                Thread.sleep(d.toMillis( ));
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread( ).interrupt( );
                return false;
            }
        }
    }

    // ------------------------------------------------------------------
    // userId existence check (mirrors ItemSynthesizer's pattern)
    // ------------------------------------------------------------------


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
            GetItemResponse response = lowLevelClient.getItem(request);
            return response.hasItem( ) && !response.item( ).isEmpty( );
        } catch (Exception e) {
            log.warn("userId existence check failed for {}: {}", userId, e.getMessage( ));
            return false;
        }
    }

    // ------------------------------------------------------------------
    // Report formatting
    // ------------------------------------------------------------------


    private static void printReport(User user, List<InventoryItem> listings)
    {
        StringBuilder sb = new StringBuilder(2048);
        sb.append("\n\n");
        sb.append("================================================================================\n");
        sb.append("  LISTING ACTIVITY REPORT\n");
        sb.append("================================================================================\n");
        sb.append("\n");

        // Contact information block
        sb.append(String.format("  User ID:    %d%n", user.getUserId( )));
        sb.append(String.format("  Name:       %s %s%n", user.getFirstName( ), user.getLastName( )));
        sb.append(String.format("  Email:      %s%n", user.getEmail( )));
        sb.append(String.format("  Phone:      %s%n", user.getPhoneNumber( )));
        sb.append("  Address:    ").append(user.getAddressLine1( )).append('\n');
        if (user.getAddressLine2( ) != null && !user.getAddressLine2( ).isBlank( )) {
            sb.append("              ").append(user.getAddressLine2( )).append('\n');
        }
        sb.append(String.format("              %s, %s %s%n",
            user.getCity( ), user.getState( ), user.getZip( )));
        sb.append("\n");
        sb.append("--------------------------------------------------------------------------------\n");

        if (listings.isEmpty( )) {
            sb.append("\n");
            sb.append("  User has not listed any items. User appears to be browse-and-buy-only\n");
            sb.append("  profile. Suggest 20% bonus on sale value of first item listed to increase\n");
            sb.append("  user profile level to consignment-participant tier.\n");
            sb.append("\n");
            sb.append("================================================================================\n");
            sb.append("\n\n");
            System.out.print(sb);
            return;
        }

        // Items table — three columns: itemId (UUID, 36 chars), title (variable),
        // price (right-aligned currency). We pad title to a fixed width and let
        // long titles run over rather than truncating; vintage titles are
        // typically well under the limit.
        final int titleWidth = 50;
        sb.append("\n");
        sb.append(String.format("  %-36s  %-" + titleWidth + "s  %12s%n",
            "ITEM ID", "TITLE", "PRICE (USD)"));
        sb.append("  ").append("-".repeat(36)).append("  ")
            .append("-".repeat(titleWidth)).append("  ")
            .append("-".repeat(12)).append('\n');

        BigDecimal total = BigDecimal.ZERO;
        for (InventoryItem item : listings) {
            BigDecimal price = item.getPrice( ) == null ? BigDecimal.ZERO : item.getPrice( );
            total = total.add(price);
            sb.append(String.format("  %-36s  %-" + titleWidth + "s  %12s%n",
                item.getItemId( ),
                truncate(item.getTitle( ), titleWidth),
                formatCurrency(price)));
        }

        sb.append("  ").append(" ".repeat(36)).append("  ")
            .append(" ".repeat(titleWidth)).append("  ")
            .append("-".repeat(12)).append('\n');
        sb.append(String.format("  %-36s  %-" + titleWidth + "s  %12s%n",
            "", "TOTAL LISTED VALUE", formatCurrency(total)));
        sb.append('\n');
        sb.append(String.format("  Items listed: %d%n", listings.size( )));
        sb.append("\n");
        sb.append("================================================================================\n");
        sb.append("\n\n");

        System.out.print(sb);
    }


    private static String formatCurrency(BigDecimal amount)
    {
        BigDecimal scaled = amount.setScale(2, RoundingMode.HALF_UP);
        // Manual thousands grouping to avoid Locale dependencies.
        String[] parts = scaled.toPlainString( ).split("\\.");
        String intPart = parts[0];
        String fracPart = parts.length > 1 ? parts[1] : "00";
        StringBuilder grouped = new StringBuilder( );
        int len = intPart.length( );
        for (int i = 0; i < len; i++) {
            if (i > 0 && (len - i) % 3 == 0) {
                grouped.append(',');
            }
            grouped.append(intPart.charAt(i));
        }
        return "$" + grouped + "." + fracPart;
    }


    private static String truncate(String s, int width)
    {
        if (s == null) return "";
        if (s.length( ) <= width) return s;
        return s.substring(0, width - 1) + "…";
    }
}