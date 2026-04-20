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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Drives continuous User creation against the DynamoDB {@code user} table.
 *
 * <p>Each worker thread runs an independent loop that:
 * <ol>
 *   <li>synthesizes a User,</li>
 *   <li>inserts it into the table,</li>
 *   <li>sleeps for the configured delay,</li>
 *   <li>repeats until the user ceiling is reached or {@link #stop()} is called.</li>
 * </ol>
 *
 * <p>The default thread count is {@link Runtime#availableProcessors()} and
 * the default per-thread delay between inserts is 1 second. Both are
 * tunable via the constructor.
 *
 * <p>All workers share a single {@link UserSynthesizer} — its internal
 * id sequence is an {@link AtomicLong}, so {@code userId} collisions cannot
 * occur regardless of worker count.
 *
 * <p>The feeder enforces a steady-state ceiling of {@value #DEFAULT_USER_CEILING}
 * users. Once reached, workers continue running but become no-ops until the
 * ceiling is raised or {@link #stop()} is called. The ceiling is enforced by
 * a pre-claim pattern on an {@link AtomicLong}: a worker reserves its slot
 * before synthesis, so concurrent workers cannot collectively overshoot.
 *
 * <p>This class has its own {@link #main(String[])} so it can be invoked
 * directly from the assembled JAR alongside the other workload personalities,
 * e.g. {@code java -cp dynafeed.jar com.aerospike.dynafeed.UserFeeder}.
 */
public final class UserFeeder
{
    private static final Logger log = LoggerFactory.getLogger(UserFeeder.class);

    private static final int DEFAULT_THREADS = Runtime.getRuntime( ).availableProcessors( );
    private static final Duration DEFAULT_DELAY = Duration.ofSeconds(1);
    private static final long DEFAULT_USER_CEILING = 5_000L;
    private static final String TABLE_NAME = "user";

    private final int threadCount;
    private final Duration delay;
    private final long userCeiling;
    private final DynamoDbTable<User> userTable;
    private final UserSynthesizer synthesizer;

    /**
     * Count of slots claimed by workers. Incremented atomically before
     * synthesis; if the claim exceeds {@link #userCeiling}, the worker
     * backs out without inserting. On insert failure, the claim is
     * released so a later attempt can retake the slot.
     */
    private final AtomicLong claimedCount = new AtomicLong( );
    private final AtomicLong successCount = new AtomicLong( );
    private final AtomicLong failureCount = new AtomicLong( );
    private volatile ExecutorService executor;
    private volatile boolean running;
    private volatile boolean ceilingLogged;


    public UserFeeder(DynamoDbEnhancedClient enhancedClient,
        UserSynthesizer synthesizer,
        int threadCount,
        Duration delay,
        long userCeiling)
    {
        if (threadCount < 1) {
            throw new IllegalArgumentException("threadCount must be >= 1");
        }
        if (delay == null || delay.isNegative( )) {
            throw new IllegalArgumentException("delay must be non-null and non-negative");
        }
        if (userCeiling < 1) {
            throw new IllegalArgumentException("userCeiling must be >= 1");
        }
        this.threadCount = threadCount;
        this.delay = delay;
        this.userCeiling = userCeiling;
        this.synthesizer = synthesizer;
        this.userTable = enhancedClient.table(TABLE_NAME, TableSchema.fromBean(User.class));
    }


    public UserFeeder(DynamoDbEnhancedClient enhancedClient,
        UserSynthesizer synthesizer,
        int threadCount,
        Duration delay)
    {
        this(enhancedClient, synthesizer, threadCount, delay, DEFAULT_USER_CEILING);
    }


    public UserFeeder(DynamoDbEnhancedClient enhancedClient, UserSynthesizer synthesizer)
    {
        this(enhancedClient, synthesizer, DEFAULT_THREADS, DEFAULT_DELAY, DEFAULT_USER_CEILING);
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
            log.warn("UserFeeder already running; ignoring start()");
            return;
        }
        log.info("Starting UserFeeder: threads={}, delay={}, ceiling={}",
            threadCount, delay, userCeiling);
        AtomicLong threadOrdinal = new AtomicLong( );
        executor = Executors.newFixedThreadPool(threadCount, r -> {
            Thread t = new Thread(r, "user-feeder-" + threadOrdinal.getAndIncrement( ));
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
        log.info("Stopping UserFeeder: successes={}, failures={}",
            successCount.get( ), failureCount.get( ));
        running = false;
        executor.shutdown( );
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("UserFeeder workers did not terminate within 10s; forcing shutdown");
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


    public long getUserCeiling( )
    {
        return userCeiling;
    }

    // ------------------------------------------------------------------
    // main
    // ------------------------------------------------------------------


    public static void main(String[ ] args) throws InterruptedException
    {
        var clientDriver = DynamoDbClient.builder()
            .region(Region.US_WEST_2)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build( );

        var enhancedClient = DynamoDbEnhancedClient.builder( )
            .dynamoDbClient(clientDriver)
            .build( );

        var synthesizer = new UserSynthesizer(1);
        var feeder = new UserFeeder(enhancedClient, synthesizer);

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
     * {@link #claimedCount} to enforce the user ceiling.
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
         * Claims a slot, synthesizes a user, and inserts it. If the
         * ceiling has been reached, backs out without work. If the insert
         * fails, releases the claim so the slot remains available.
         */
        private void tryInsertOnce( )
        {
            long claim = claimedCount.incrementAndGet( );
            if (claim > userCeiling) {
                claimedCount.decrementAndGet( );
                if (!ceilingLogged) {
                    ceilingLogged = true;
                    log.info("User ceiling of {} reached; feeder now idling", userCeiling);
                }
                return;
            }

            try {
                User user = synthesizer.synthesize( );
                userTable.putItem(user);
                successCount.incrementAndGet( );
            } catch (Exception e) {
                failureCount.incrementAndGet( );
                claimedCount.decrementAndGet( );
                // Log at warn, not error — in a sustained load test we
                // expect occasional throttling exceptions, and we don't
                // want to drown the log in stack traces.
                log.warn("User insert failed: {}", e.getMessage( ));
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