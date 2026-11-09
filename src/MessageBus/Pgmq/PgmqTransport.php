<?php

declare(strict_types=1);

namespace Thesis\MessageBus\Pgmq;

use Amp\DeferredFuture;
use Amp\Pipeline;
use Amp\Postgres\PostgresConnection;
use Amp\Postgres\PostgresLink;
use Revolt\EventLoop;
use Thesis\Headers;
use Thesis\Headers\BoolHeader;
use Thesis\MessageBus\Pgmq\Internal\PgmqConsumer;
use Thesis\MessageBus\Protocol\DeserializationFailed;
use Thesis\MessageBus\Transport\ConsumerHandler;
use Thesis\MessageBus\Transport\Disposition;
use Thesis\MessageBus\Transport\InboundEnvelope;
use Thesis\MessageBus\Transport\OutboundEnvelope;
use Thesis\MessageBus\Transport\Receiver;
use Thesis\MessageBus\Transport\SubscriptionConfigurator;
use Thesis\MessageBus\Transport\TransactionalDispatcher;
use Thesis\Pgmq;
use Thesis\Pgmq\Internal\AggregateWatcher;
use Thesis\Pgmq\Internal\ChannelWatcher;
use Thesis\Pgmq\Internal\TimeoutWatcher;
use Thesis\Pgmq\Message;
use Thesis\Time\TimeSpan;

/**
 * @api
 *
 * @implements TransactionalDispatcher<PostgresLink>
 */
final readonly class PgmqTransport implements TransactionalDispatcher, Receiver, SubscriptionConfigurator
{
    private TimeSpan $visibilityTimeout;

    private TimeSpan $pollInterval;

    private BoolHeader $payloadJsonEncodedHeader;

    /**
     * @param positive-int $batchSize
     */
    public function __construct(
        private PostgresConnection $postgres,
        private int $batchSize = 10,
        ?TimeSpan $visibilityTimeout = null,
        ?TimeSpan $pollInterval = null,
        private TimeSpan $requeueDelay = new TimeSpan(0),
    ) {
        $this->visibilityTimeout = $visibilityTimeout ?? TimeSpan::fromSeconds(30);
        $this->pollInterval = $pollInterval ?? TimeSpan::fromSeconds(5);
        $this->payloadJsonEncodedHeader = new BoolHeader('thesis-pgmq-payload-json-encoded');
    }

    public function createQueue(string $name): void
    {
        Pgmq\createExtension($this->postgres);
        Pgmq\createQueue($this->postgres, $name);

        $this->postgres->query('create schema if not exists thesis_message_bus;');
        $this->postgres->query(
            <<<'SQL'
                create or replace function thesis_message_bus.pgmq_dispatch_v1(
                    operations text[],
                    addresses text[],
                    payloads jsonb[],
                    headers jsonb[],
                    delays integer[]
                )
                returns setof bigint
                language plpgsql
                as $$
                declare
                    index integer;
                begin
                    for index in 1..coalesce(array_length(operations, 1), 0) loop
                        if operations[index] = 'send' then
                            return query select id from pgmq.send(addresses[index], payloads[index], headers[index], delays[index]) as id;
                        elsif operations[index] = 'publish' then
                            return next pgmq.send_topic(addresses[index], payloads[index], headers[index], delays[index])::bigint;
                        else
                            raise exception 'unknown message bus operation: %', operations[index];
                        end if;
                    end loop;
                end;
                $$;
                SQL,
        );
    }

    public function subscribe(string $queue, array $messageTypes): void
    {
        $boundEventTypes = [];

        /** @var array{pattern: non-empty-string} $row */
        foreach ($this->postgres->execute('select pattern from pgmq.list_topic_bindings(?)', [$queue]) as $row) {
            $boundEventTypes[] = $row['pattern'];
        }

        foreach (array_diff($messageTypes, $boundEventTypes) as $eventType) {
            Pgmq\bindTopic($this->postgres, $eventType, $queue);
        }

        foreach (array_diff($boundEventTypes, $messageTypes) as $eventType) {
            Pgmq\unbindTopic($this->postgres, $eventType, $queue);
        }
    }

    public function dispatch(array $envelopes): void
    {
        $this->dispatchInTransaction($this->postgres, $envelopes);
    }

    public function dispatchInTransaction(object $transaction, array $envelopes): void
    {
        $operations = [];
        $addresses = [];
        $payloads = [];
        $headers = [];
        $delays = [];

        foreach ($envelopes as $envelope) {
            [$payloads[], $headers[]] = $this->encode($envelope);
            $operations[] = $envelope->operation->value;
            $addresses[] = $envelope->address;
            $delays[] = $envelope->delay->toSeconds();
        }

        $result = $transaction->execute(
            <<<'SQL'
                select thesis_message_bus.pgmq_dispatch_v1(
                    :operations::text[],
                    :addresses::text[],
                    :payloads::jsonb[],
                    :headers::jsonb[],
                    :delays::int[]
                )
                SQL,
            [
                'operations' => $operations,
                'addresses' => $addresses,
                'payloads' => $payloads,
                'headers' => $headers,
                'delays' => $delays,
            ],
        );

        foreach ($result as $_);
    }

    /**
     * @return array{non-empty-string, non-empty-string}
     */
    private function encode(OutboundEnvelope $envelope): array
    {
        $headers = $envelope->headers;
        $payload = $envelope->payload;

        if (!json_validate($payload)) {
            $payload = json_encode($payload, JSON_THROW_ON_ERROR);
            $headers = $headers->with($this->payloadJsonEncodedHeader, true);
        }

        return [
            $payload,
            json_encode($headers->encode(), JSON_THROW_ON_ERROR),
        ];
    }

    public function startConsumer(string $queue, ConsumerHandler $handler): PgmqConsumer
    {
        $queue = Pgmq\findQueue($this->postgres, $queue);

        /** @var DeferredFuture<void> $completion */
        $completion = new DeferredFuture();

        /** @var Pipeline\Queue<null> $polls */
        $polls = new Pipeline\Queue(1);
        $polls->push(null);

        $timeoutWatcher = $this->pollInterval->isPositive()
            ? new TimeoutWatcher($polls, $this->pollInterval)
            : throw new \LogicException('Polling interval must be positive.');

        $watcher = new AggregateWatcher([
            $timeoutWatcher,
            new ChannelWatcher(
                $polls,
                $this->postgres->listen($queue->enableNotifyInsert()),
                $timeoutWatcher,
            ),
        ]);

        $iterator = $polls->iterate();

        EventLoop::queue(function () use ($queue, $handler, $completion, $watcher, $iterator, $polls): void {
            $watcher->watch();

            try {
                while ($iterator->continue()) {
                    $messages = [...$queue->readBatch(
                        count: $this->batchSize,
                        visibilityTimeout: $this->visibilityTimeout,
                    )];

                    if ($messages === []) {
                        continue;
                    }

                    /** @var non-empty-array<int, int> */
                    $unhandledMessageIds = array_column($messages, 'id', 'id');

                    $updateVisibilityId = EventLoop::repeat(
                        interval: $this->visibilityTimeout->toSeconds(precision: 4) / 2,
                        closure: function () use ($queue, &$unhandledMessageIds): void {
                            $queue->setVisibilityTimeout(
                                messageIds: array_values($unhandledMessageIds),
                                visibilityTimeout: $this->visibilityTimeout,
                            );
                        },
                    );

                    try {
                        foreach ($messages as $message) {
                            match ($handler->handle($this->decode($message))) {
                                Disposition::Ack, Disposition::Nack => $queue->delete($message->id),
                                Disposition::Requeue => $queue->setVisibilityTimeout(
                                    messageIds: [$message->id],
                                    visibilityTimeout: $this->requeueDelay,
                                ),
                            };

                            unset($unhandledMessageIds[$message->id]);
                        }
                    } finally {
                        EventLoop::cancel($updateVisibilityId);
                    }

                    // A full batch means the queue may hold more messages. Inserts that already happened will not
                    // notify again, so waiting for the next signal would leave the backlog idle for up to $pollInterval.
                    if (\count($messages) === $this->batchSize && !$polls->isComplete()) {
                        $polls->pushAsync(null)->ignore();
                    }
                }

                $completion->complete();
            } catch (\Throwable $error) {
                $watcher->cancel();
                $iterator->dispose();
                $completion->error($error);
            }
        });

        return new PgmqConsumer(
            polls: $polls,
            watcher: $watcher,
            completion: $completion->getFuture(),
        );
    }

    private function decode(Message $message): InboundEnvelope
    {
        $rawHeaders = json_decode($message->headers ?? '{}', associative: true, flags: JSON_THROW_ON_ERROR);

        if (!\is_array($rawHeaders) || !array_all($rawHeaders, static fn(mixed $value) => \is_string($value))) {
            throw new DeserializationFailed('PGMQ message headers must be a JSON object with string values.');
        }

        $payload = $message->value;
        /** @var array<string, string> $rawHeaders */
        $headers = new Headers($rawHeaders);

        if ($headers->find($this->payloadJsonEncodedHeader) === true) {
            $payload = json_decode($message->value, associative: false, flags: JSON_THROW_ON_ERROR);

            if (!\is_string($payload)) {
                throw new \UnexpectedValueException('PGMQ raw-string payload must be encoded as a JSON string.');
            }
        }

        return new InboundEnvelope(
            payload: $payload,
            headers: $headers->without($this->payloadJsonEncodedHeader),
        );
    }
}
