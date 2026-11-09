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
