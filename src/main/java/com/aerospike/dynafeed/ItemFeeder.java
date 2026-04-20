package com.aerospike.dynafeed;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Drives continuous InventoryItem creation against the DynamoDB
 * {@code inventory} table.
 *
 * <p>Each worker thread runs an independent loop that:
 * <ol>
 *   <li>asks the synthesizer for an item (which may return empty if no
 *       valid {@code listingUser} can be found),</li>
 *   <li>inserts the item into the table when present,</li>
 *   <li>sleeps for the configured delay,</li>
 *   <li>repeats until the item ceiling is reached or {@link #stop()} is called.</li>
 * </ol>
 *
 * <p>The default thread count is {@link Runtime#availableProcessors()} and
 * the default per-thread delay between inserts is 20 ms — items churn far
 * more frequently than users, so the steady-state insertion rate is
 * correspondingly higher. Both are tunable via the constructor.
 *
 * <p>All workers share a single {@link ItemSynthesizer}. The synthesizer
 * is itself thread-safe (it holds only an immutable {@code DynamoDbClient}
 * reference and uses {@link java.util.concurrent.ThreadLocalRandom}), so
 * no per-worker instance is required.
 *
 * <p>The feeder enforces a steady-state ceiling of {@value #DEFAULT_ITEM_CEILING}
 * items via the same pre-claim pattern as {@link UserFeeder}. When the item
 * feeder eventually grows to perform deletes (items selling), the ceiling
 * logic will need to switch from monotonic claim-counting to a live
 * inventory count that can decrement.
 *
 * <p>This class has its own {@link #main(String[])} so it can be invoked
 * directly from the assembled JAR alongside the other workload personalities,
 * e.g. {@code java -cp dynafeed.jar com.aerospike.dynafeed.ItemFeeder}.
 */
public final class ItemFeeder
{
    private static final Logger log = LoggerFactory.getLogger(ItemFeeder.class);

    private static final int DEFAULT_THREADS = Runtime.getRuntime( ).availableProcessors( );
    private static final Duration DEFAULT_DELAY = Duration.ofMillis(20);
    private static final long DEFAULT_ITEM_CEILING = 5_000L;
    private static final String TABLE_NAME = "inventory";

    private final int threadCount;
    private final Duration delay;
    private final long itemCeiling;
    private final DynamoDbTable<InventoryItem> inventoryTable;
    private final ItemSynthesizer synthesizer;

    /**
     * Count of slots claimed by workers. Incremented atomically before
     * synthesis; if the claim exceeds {@link #itemCeiling}, the worker
     * backs out without inserting. On synthesis-skip or insert failure,
     * the claim is released so a later attempt can retake the slot.
     */
    private final AtomicLong claimedCount = new AtomicLong( );
    private final AtomicLong successCount = new AtomicLong( );
    private final AtomicLong failureCount = new AtomicLong( );
    private final AtomicLong skippedCount = new AtomicLong( );
    private volatile ExecutorService executor;
    private volatile boolean running;
    private volatile boolean ceilingLogged;


    public ItemFeeder(DynamoDbEnhancedClient enhancedClient,
        ItemSynthesizer synthesizer,
        int threadCount,
        Duration delay,
        long itemCeiling)
    {
        if (threadCount < 1) {
            throw new IllegalArgumentException("threadCount must be >= 1");
        }
        if (delay == null || delay.isNegative( )) {
            throw new IllegalArgumentException("delay must be non-null and non-negative");
        }
        if (itemCeiling < 1) {
            throw new IllegalArgumentException("itemCeiling must be >= 1");
        }
        this.threadCount = threadCount;
        this.delay = delay;
        this.itemCeiling = itemCeiling;
        this.synthesizer = synthesizer;
        this.inventoryTable = enhancedClient.table(TABLE_NAME,
            TableSchema.fromBean(InventoryItem.class));
    }


    public ItemFeeder(DynamoDbEnhancedClient enhancedClient,
        ItemSynthesizer synthesizer,
        int threadCount,
        Duration delay)
    {
        this(enhancedClient, synthesizer, threadCount, delay, DEFAULT_ITEM_CEILING);
    }


    public ItemFeeder(DynamoDbEnhancedClient enhancedClient, ItemSynthesizer synthesizer)
    {
        this(enhancedClient, synthesizer, DEFAULT_THREADS, DEFAULT_DELAY, DEFAULT_ITEM_CEILING);
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------


    /**
     * Starts the worker pool. Idempotent — a second call is a no-op.
     */
    public synchronized void start( )
    {
        if (running) {
            log.warn("ItemFeeder already running; ignoring start()");
            return;
        }
        log.info("Starting ItemFeeder: threads={}, delay={}, ceiling={}",
            threadCount, delay, itemCeiling);
        AtomicLong threadOrdinal = new AtomicLong( );
        executor = Executors.newFixedThreadPool(threadCount, r -> {
            Thread t = new Thread(r, "item-feeder-" + threadOrdinal.getAndIncrement( ));
            t.setDaemon(true);
            return t;
        });
        running = true;
        for (int i = 0; i < threadCount; i++) {
            executor.submit(new FeederTask( ));
        }
    }


    /**
     * Signals all workers to stop and waits up to 10 seconds for them to
     * drain. Workers finish their in-flight insert before exiting.
     */
    public synchronized void stop( )
    {
        if (!running) {
            return;
        }
        log.info("Stopping ItemFeeder: successes={}, failures={}, skipped={}",
            successCount.get( ), failureCount.get( ), skippedCount.get( ));
        running = false;
        executor.shutdown( );
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("ItemFeeder workers did not terminate within 10s; forcing shutdown");
                executor.shutdownNow( );
            }
        } catch (InterruptedException e) {
            Thread.currentThread( ).interrupt( );
            executor.shutdownNow( );
        }
    }


    public long getSuccessCount( )
    {
        return successCount.get( );
    }


    public long getFailureCount( )
    {
        return failureCount.get( );
    }


    public long getSkippedCount( )
    {
        return skippedCount.get( );
    }


    public long getItemCeiling( )
    {
        return itemCeiling;
    }

    // ------------------------------------------------------------------
    // main
    // ------------------------------------------------------------------


    public static void main(String[ ] args) throws InterruptedException
    {
        var clientDriver = DynamoDbClient.builder()
            .region(Region.US_WEST_2)
            .credentialsProvider(DefaultCredentialsProvider.create( ))
            .build();

        var enhancedClient = DynamoDbEnhancedClient.builder( )
            .dynamoDbClient(clientDriver)
            .build( );

        var synthesizer = new ItemSynthesizer(clientDriver);
        var feeder = new ItemFeeder(enhancedClient, synthesizer,
            DEFAULT_THREADS, Duration.ofMillis(20), 100_000L);

        Runtime.getRuntime( ).addShutdownHook(new Thread(( ) -> {
            feeder.stop( );
            clientDriver.close( );
        }));

        feeder.start( );

        Thread.currentThread( ).join( );
    }

    // ------------------------------------------------------------------
    // Worker
    // ------------------------------------------------------------------

    /**
     * The per-thread insertion loop. Reads the volatile {@code running}
     * flag each iteration to allow cooperative shutdown, and the
     * {@link #claimedCount} to enforce the item ceiling.
     */
    private final class FeederTask implements Runnable
    {

        @Override
        public void run( )
        {
            log.debug("FeederTask started on {}", Thread.currentThread( ).getName( ));
            while (running) {
                tryInsertOnce( );
                if (!sleepQuietly(delay)) {
                    break; // interrupted — exit the loop
                }
            }
            log.debug("FeederTask exiting on {}", Thread.currentThread( ).getName( ));
        }


        /**
         * Claims a slot, asks the synthesizer for an item, and inserts it.
         * If the ceiling has been reached, backs out without work. If the
         * synthesizer skips or the insert fails, releases the claim so the
         * slot remains available.
         */
        private void tryInsertOnce( )
        {
            long claim = claimedCount.incrementAndGet( );
            if (claim > itemCeiling) {
                claimedCount.decrementAndGet( );
                if (!ceilingLogged) {
                    ceilingLogged = true;
                    log.info("Item ceiling of {} reached; feeder now idling", itemCeiling);
                }
                return;
            }

            Optional<InventoryItem> maybeItem;
            try {
                maybeItem = synthesizer.synthesize( );
            } catch (Exception e) {
                failureCount.incrementAndGet( );
                claimedCount.decrementAndGet( );
                log.warn("Item synthesis failed: {}", e.getMessage( ));
                return;
            }

            if (maybeItem.isEmpty( )) {
                // Synthesizer already INFO-logged the reason. Release the
                // claim so the slot is available for a later attempt.
                skippedCount.incrementAndGet( );
                claimedCount.decrementAndGet( );
                return;
            }

            try {
                inventoryTable.putItem(maybeItem.get( ));
                successCount.incrementAndGet( );
            } catch (Exception e) {
                failureCount.incrementAndGet( );
                claimedCount.decrementAndGet( );
                // Log at warn, not error — under sustained load we expect
                // occasional throttling exceptions, and we don't want to
                // drown the log in stack traces.
                log.warn("Item insert failed: {}", e.getMessage( ));
            }
        }


        /**
         * Sleeps for the given duration. Returns false if interrupted,
         * in which case the caller should exit its loop.
         */
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
}
