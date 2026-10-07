<?php

declare(strict_types=1);

namespace Thesis\MessageBus\Pgmq;

use Amp\DeferredFuture;
use Amp\Future;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PostgresConnectionPool;
use Amp\TimeoutCancellation;
use Testo\Assert;
use Testo\Assert\ExpectNoAssertions;
use Testo\Codecov\Covers;
use Testo\Test;
use Thesis\Headers;
use Thesis\MessageBus\Transport\Consumer;
use Thesis\MessageBus\Transport\ConsumerHandler;
use Thesis\MessageBus\Transport\Disposition;
use Thesis\MessageBus\Transport\InboundEnvelope;
use Thesis\MessageBus\Transport\Operation;
use Thesis\MessageBus\Transport\OutboundEnvelope;
use Thesis\Pgmq;
use Thesis\Time\TimeSpan;
use function Amp\async;
use function Amp\delay;

#[Test]
#[Covers(PgmqTransport::class)]
final readonly class PgmqNotifyTriggerTest
{
    #[ExpectNoAssertions]
    public function consumerStartDoesNotWaitForProducerTransactions(): void
    {
        $postgres = self::postgres();
        $transport = new PgmqTransport($postgres);
        $queue = self::createQueue($transport);

        // A producer is in the middle of a transaction that has already inserted into the queue.
        $producer = $postgres->beginTransaction();
        $transport->dispatchInTransaction($producer, [self::envelope($queue)]);

        // A worker (re)starts. Recreating the notify trigger takes a table lock that waits for every open producer
        // transaction and blocks new inserts meanwhile, so the consumer must not touch it.
        /** @var Future<Consumer> $start */
        $start = async(static fn(): Consumer => $transport->startConsumer($queue, new SignallingHandler()));

        try {
            $start->await(new TimeoutCancellation(1));
        } finally {
            $producer->rollback();

            $consumer = $start->await();
            $consumer->stop();
            $consumer->awaitCompletion();
            Pgmq\dropQueue($postgres, $queue);
        }
    }

    public function createdQueueWakesConsumerOnInsert(): void
    {
        $postgres = self::postgres();
        $transport = new PgmqTransport($postgres, pollInterval: TimeSpan::fromSeconds(10));
        $queue = self::createQueue($transport);

        $handler = new SignallingHandler();
        $consumer = $transport->startConsumer($queue, $handler);

        try {
            // Let the consumer make its initial (empty) read first, so only a notification can deliver the message.
            delay(0.2);
            $transport->dispatch([self::envelope($queue)]);

            // Notifications are set up by createQueue(): the insert wakes the consumer long before the poll interval.
            $handler->received->getFuture()->await(new TimeoutCancellation(2));
        } finally {
            $consumer->stop();
            $consumer->awaitCompletion();
            Pgmq\dropQueue($postgres, $queue);
        }

        Assert::true($handler->received->isComplete());
    }

    public function consumerStartRestoresNotificationsLostInCrashRecovery(): void
    {
        $postgres = self::postgres();
        $transport = new PgmqTransport($postgres, pollInterval: TimeSpan::fromSeconds(10));
        $queue = self::createQueue($transport);

        // pgmq.notify_insert_throttle is UNLOGGED: crash recovery (or a promoted replica) leaves it empty, and the
        // trigger notifies only when it finds the queue's row. A consumer (re)start must bring notifications back.
        $postgres->execute('DELETE FROM pgmq.notify_insert_throttle WHERE queue_name = ?', [$queue]);

        $handler = new SignallingHandler();
        $consumer = $transport->startConsumer($queue, $handler);

        try {
            // Let the consumer make its initial (empty) read first, so only a notification can deliver the message.
            delay(0.2);
            $transport->dispatch([self::envelope($queue)]);

            $handler->received->getFuture()->await(new TimeoutCancellation(2));
        } finally {
            $consumer->stop();
            $consumer->awaitCompletion();
            Pgmq\dropQueue($postgres, $queue);
        }

        Assert::true($handler->received->isComplete());
    }

    public function concurrentConsumerStartsCreateTheTriggerOnce(): void
    {
        $postgres = self::postgres();
        $transport = new PgmqTransport($postgres);
        $queue = self::createQueue($transport);
        // Simulates a queue whose notifications are not set up (created before, or lost).
        $postgres->execute('SELECT pgmq.disable_notify_insert(?)', [$queue]);

        // A producer keeps an insert into the queue open, so the consumer starts are guaranteed to overlap.
        $producer = $postgres->beginTransaction();
        $transport->dispatchInTransaction($producer, [self::envelope($queue)]);

        $failures = [];
        $creations = self::countTriggerCreations($postgres, $queue, static function () use ($producer, $queue, &$failures): void {
            $starts = [];

            for ($i = 0; $i < 4; ++$i) {
                // Each consumer has its own pool, as separate worker processes do.
                $own = new PgmqTransport(self::postgres());
                $starts[] = async(static fn(): Consumer => $own->startConsumer($queue, new SignallingHandler()));
            }

            delay(0.5);
            $producer->rollback();

            foreach ($starts as $start) {
                try {
                    /** @var Consumer $consumer */
                    $consumer = $start->await(new TimeoutCancellation(5));
                    $consumer->stop();
                    $consumer->awaitCompletion();
                } catch (\Throwable $e) {
                    $failures[] = $e->getMessage();
                }
            }
        });

        Pgmq\dropQueue($postgres, $queue);

        Assert::same($failures, []);
        Assert::same($creations, 1);
    }

    public function consumerStartRestoresDisabledTrigger(): void
    {
        $postgres = self::postgres();
        $transport = new PgmqTransport($postgres, pollInterval: TimeSpan::fromSeconds(10));
        $queue = self::createQueue($transport);
        $postgres->execute('SELECT pgmq.enable_notify_insert(?, 30)', [$queue]);
        $postgres->query(\sprintf('ALTER TABLE pgmq.q_%s DISABLE TRIGGER trigger_notify_queue_insert_listeners', $queue));

        $handler = new SignallingHandler();
        $consumer = $transport->startConsumer($queue, $handler);

        try {
            // Let the consumer make its initial (empty) read first, so only a notification can deliver the message.
            delay(0.2);
            $transport->dispatch([self::envelope($queue)]);

            $handler->received->getFuture()->await(new TimeoutCancellation(2));
        } finally {
            $consumer->stop();
            $consumer->awaitCompletion();
            Pgmq\dropQueue($postgres, $queue);
        }

        Assert::true($handler->received->isComplete());
    }

    /**
     * Runs $action and returns how many times the notify trigger of the queue was created meanwhile (event trigger).
     *
     * @param non-empty-string $queue
     * @param callable(): void $action
     */
    private static function countTriggerCreations(PostgresConnection $postgres, string $queue, callable $action): int
    {
        $postgres->query('CREATE TABLE IF NOT EXISTS public.thesis_test_ddl (object_identity text)');
        $postgres->query('TRUNCATE public.thesis_test_ddl');
        $postgres->query(<<<'SQL'
            CREATE OR REPLACE FUNCTION public.thesis_test_log_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$
            DECLARE r record;
            BEGIN
                FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
                    INSERT INTO public.thesis_test_ddl (object_identity) VALUES (r.object_identity);
                END LOOP;
            END $$
            SQL);
        $postgres->query('DROP EVENT TRIGGER IF EXISTS thesis_test_ddl');
        $postgres->query("CREATE EVENT TRIGGER thesis_test_ddl ON ddl_command_end WHEN TAG IN ('CREATE TRIGGER') EXECUTE FUNCTION public.thesis_test_log_ddl()");

        try {
            $action();
        } finally {
            $postgres->query('DROP EVENT TRIGGER IF EXISTS thesis_test_ddl');
        }

        /** @var array{n: int} $row */
        $row = $postgres
            ->execute('SELECT count(*)::int AS n FROM public.thesis_test_ddl WHERE object_identity LIKE ?', ['% on pgmq.q_' . $queue])
            ->fetchRow();

        return $row['n'];
    }

    private static function postgres(): PostgresConnection
    {
        $dsn = getenv('THESIS_PGMQ_DSN');

        if (!\is_string($dsn) || $dsn === '') {
            throw new \LogicException('Set the THESIS_PGMQ_DSN environment variable (see compose.yaml).');
        }

        return new PostgresConnectionPool(PostgresConfig::fromString($dsn));
    }

    /**
     * @return non-empty-string
     */
    private static function createQueue(PgmqTransport $transport): string
    {
        $queue = 'q' . bin2hex(random_bytes(8));
        $transport->createQueue($queue);

        return $queue;
    }

    /**
     * @param non-empty-string $queue
     */
    private static function envelope(string $queue): OutboundEnvelope
    {
        return new OutboundEnvelope(
            operation: Operation::Send,
            address: $queue,
            payload: '{}',
            headers: new Headers(),
        );
    }
}

/**
 * @internal
 */
final readonly class SignallingHandler implements ConsumerHandler
{
    /** @var DeferredFuture<null> */
    public DeferredFuture $received;

    public function __construct()
    {
        $this->received = new DeferredFuture();
    }

    #[\Override]
    public function handle(InboundEnvelope $envelope): Disposition
    {
        if (!$this->received->isComplete()) {
            $this->received->complete();
        }

        return Disposition::Ack;
    }
}
