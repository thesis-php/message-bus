<?php

declare(strict_types=1);

namespace Thesis\MessageBus\Pgmq;

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
use Thesis\MessageBus\Transport\Operation;
use Thesis\MessageBus\Transport\OutboundEnvelope;
use Thesis\Pgmq;
use Thesis\Time\TimeSpan;
use function Amp\async;
use function Amp\delay;

#[Test]
#[Covers(PgmqTransport::class)]
final readonly class PgmqDispatchNotifyTest
{
    #[ExpectNoAssertions]
    public function sendersToOneQueueDoNotWaitForEachOther(): void
    {
        $postgres = self::postgres();
        $transport = new PgmqTransport($postgres);
        $queue = self::createQueue($transport);
        // A live consumer, as in production: with insert notifications done by pgmq's trigger it is the consumer start
        // that sets the trigger up.
        $consumer = $transport->startConsumer($queue, new SignallingHandler());

        // A sender keeps its transaction open after sending, as a handler does while it finishes its unit of work.
        $first = $postgres->beginTransaction();
        $transport->dispatchInTransaction($first, [self::send($queue)]);

        try {
            /** @var Future<null> $second */
            $second = async(static function () use ($transport, $queue): void {
                $transport->dispatch([self::send($queue)]);
            });
            $second->await(new TimeoutCancellation(1));
        } finally {
            $first->rollback();
            $consumer->stop();
            $consumer->awaitCompletion();
            Pgmq\dropQueue($postgres, $queue);
        }
    }

    public function crosswiseSendsToTwoQueuesDoNotDeadlock(): void
    {
        $postgres = self::postgres();
        $transport = new PgmqTransport($postgres);
        $one = self::createQueue($transport);
        $two = self::createQueue($transport);
        $consumers = [
            $transport->startConsumer($one, new SignallingHandler()),
            $transport->startConsumer($two, new SignallingHandler()),
        ];

        $a = $postgres->beginTransaction();
        $b = $postgres->beginTransaction();
        $failures = [];

        try {
            $transport->dispatchInTransaction($a, [self::send($one)]);
            $transport->dispatchInTransaction($b, [self::send($two)]);

            $aToTwo = async(static fn() => $transport->dispatchInTransaction($a, [self::send($two)]));
            delay(0.1);
            $bToOne = async(static fn() => $transport->dispatchInTransaction($b, [self::send($one)]));

            foreach ([$aToTwo, $bToOne] as $future) {
                try {
                    $future->await(new TimeoutCancellation(3));
                } catch (\Throwable $e) {
                    $failures[] = strtok($e->getMessage(), "\n");
                }
            }

            foreach ([$a, $b] as $transaction) {
                if ($transaction->isActive()) {
                    $transaction->commit();
                }
            }
        } finally {
            foreach ($consumers as $consumer) {
                $consumer->stop();
                $consumer->awaitCompletion();
            }
            Pgmq\dropQueue($postgres, $one);
            Pgmq\dropQueue($postgres, $two);
        }

        Assert::same($failures, []);
    }

    public function publishWakesSubscribedConsumerOnCommitOnly(): void
    {
        $postgres = self::postgres();
        $transport = new PgmqTransport($postgres, pollInterval: TimeSpan::fromSeconds(10));
        $queue = self::createQueue($transport);
        $topic = 'test.' . $queue;
        $transport->subscribe($queue, [$topic]);

        $handler = new SignallingHandler();
        $consumer = $transport->startConsumer($queue, $handler);

        try {
            // Let the consumer make its initial (empty) read first, so only a notification can deliver the message.
            delay(0.2);
            $tx = $postgres->beginTransaction();
            $transport->dispatchInTransaction($tx, [self::publish($topic)]);

            // Not committed yet: nothing is announced.
            delay(0.5);
            Assert::false($handler->received->isComplete());

            $tx->commit();
            $handler->received->getFuture()->await(new TimeoutCancellation(2));
        } finally {
            $consumer->stop();
            $consumer->awaitCompletion();
            Pgmq\dropQueue($postgres, $queue);
        }

        Assert::true($handler->received->isComplete());
    }

    public function createQueueDropsTheInsertTriggerOfAnEarlierSetup(): void
    {
        $postgres = self::postgres();
        $transport = new PgmqTransport($postgres);
        $queue = self::createQueue($transport);
        $postgres->execute('SELECT pgmq.enable_notify_insert(?, 30)', [$queue]);

        try {
            $transport->createQueue($queue);

            /** @var array{triggers: int, rows: int} $row */
            $row = $postgres->execute(
                <<<'SQL'
                    SELECT
                        (SELECT count(*)::int FROM pg_trigger tg JOIN pg_class c ON c.oid = tg.tgrelid
                         WHERE c.relname = pgmq.format_table_name(:queue, 'q')
                           AND tg.tgname = 'trigger_notify_queue_insert_listeners') AS triggers,
                        (SELECT count(*)::int FROM pgmq.notify_insert_throttle WHERE queue_name = :queue) AS rows
                    SQL,
                ['queue' => $queue],
            )->fetchRow();
        } finally {
            Pgmq\dropQueue($postgres, $queue);
        }

        Assert::same($row, ['triggers' => 0, 'rows' => 0]);
    }

    public function createQueueDoesNotStallABusyQueueWhileUndoingAnEarlierSetup(): void
    {
        $postgres = self::postgres();
        $transport = new PgmqTransport($postgres);
        $queue = self::createQueue($transport);
        $postgres->execute('SELECT pgmq.enable_notify_insert(?, 30)', [$queue]);

        // A long transaction has read the queue and is still open. Its lock does not stop pgmq.create() of the existing
        // queue, but DROP TRIGGER (ACCESS EXCLUSIVE) would wait for it, and every reader and sender behind that.
        $sender = $postgres->beginTransaction();
        $sender->query(\sprintf('SELECT count(*) FROM pgmq.%s', 'q_' . $queue));

        try {
            $startedAt = microtime(true);
            $transport->createQueue($queue);
            $took = microtime(true) - $startedAt;

            // The throttle row is gone even though the trigger could not be dropped: a send no longer waits for anyone.
            /** @var array{rows: int} $row */
            $row = $postgres->execute('SELECT count(*)::int AS rows FROM pgmq.notify_insert_throttle WHERE queue_name = ?', [$queue])->fetchRow();
            $other = $postgres->beginTransaction();
            $transport->dispatchInTransaction($other, [self::send($queue)]);
            $third = async(static fn() => $transport->dispatch([self::send($queue)]));
            $third->await(new TimeoutCancellation(1));
            $other->rollback();
        } finally {
            $sender->rollback();
            Pgmq\dropQueue($postgres, $queue);
        }

        Assert::true($took < 4.0);
        Assert::same($row, ['rows' => 0]);
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
    private static function send(string $queue): OutboundEnvelope
    {
        return new OutboundEnvelope(operation: Operation::Send, address: $queue, payload: '{}', headers: new Headers());
    }

    /**
     * @param non-empty-string $topic
     */
    private static function publish(string $topic): OutboundEnvelope
    {
        return new OutboundEnvelope(operation: Operation::Publish, address: $topic, payload: '{}', headers: new Headers());
    }
}
