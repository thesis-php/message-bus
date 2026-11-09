<?php

declare(strict_types=1);

namespace Thesis\MessageBus\Pgmq;

use Amp\DeferredFuture;
use Amp\Postgres\PostgresConfig;
use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PostgresConnectionPool;
use Amp\TimeoutCancellation;
use Testo\Assert;
use Testo\Codecov\Covers;
use Testo\Test;
use Thesis\Headers;
use Thesis\MessageBus\Transport\ConsumerHandler;
use Thesis\MessageBus\Transport\Disposition;
use Thesis\MessageBus\Transport\InboundEnvelope;
use Thesis\MessageBus\Transport\Operation;
use Thesis\MessageBus\Transport\OutboundEnvelope;
use Thesis\Pgmq;
use Thesis\Time\TimeSpan;

#[Test]
#[Covers(PgmqTransport::class)]
final readonly class PgmqTransportTest
{
    public function consumerDrainsBacklogWithoutWaitingForPollInterval(): void
    {
        $postgres = self::postgres();
        $queue = self::createQueue($postgres);
        $transport = new PgmqTransport($postgres, batchSize: 10, pollInterval: TimeSpan::fromSeconds(10));
        $transport->dispatch(self::envelopes($queue, 25));

        $handler = new CountingHandler(25);
        $consumer = $transport->startConsumer($queue, $handler);

        try {
            // The backlog is already there: no further inserts will wake the consumer up, so a full batch must
            // trigger the next read immediately instead of waiting for the poll interval (2 x 10 s for 25 messages).
            $handler->done->getFuture()->await(new TimeoutCancellation(2));
        } finally {
            $consumer->stop();
            $consumer->awaitCompletion();
            Pgmq\dropQueue($postgres, $queue);
        }

        Assert::same($handler->handled, 25);
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
    private static function createQueue(PostgresConnection $postgres): string
    {
        $queue = 'q' . bin2hex(random_bytes(8));
        new PgmqTransport($postgres)->createQueue($queue);

        return $queue;
    }

    /**
     * @param non-empty-string $queue
     * @param positive-int $count
     * @return non-empty-list<OutboundEnvelope>
     */
    private static function envelopes(string $queue, int $count): array
    {
        return array_map(
            static fn(int $index): OutboundEnvelope => new OutboundEnvelope(
                operation: Operation::Send,
                address: $queue,
                payload: json_encode(['index' => $index], JSON_THROW_ON_ERROR),
                headers: new Headers(),
            ),
            range(1, $count),
        );
    }
}

/**
 * @internal
 */
final class CountingHandler implements ConsumerHandler
{
    public int $handled = 0;

    /** @var DeferredFuture<null> */
    public readonly DeferredFuture $done;

    /**
     * @param positive-int $expected
     */
    public function __construct(
        private readonly int $expected,
    ) {
        $this->done = new DeferredFuture();
    }

    #[\Override]
    public function handle(InboundEnvelope $envelope): Disposition
    {
        ++$this->handled;

        if ($this->handled === $this->expected) {
            $this->done->complete();
        }

        return Disposition::Ack;
    }
}
