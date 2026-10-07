<?php

declare(strict_types=1);

namespace Thesis\MessageBus\Handling;

use Thesis\MessageBus\HandlerContext;

/**
 * @api
 *
 * @template Tx of object = object
 * @implements HandlerRegistry<Tx>
 */
final class Handlers implements HandlerRegistry
{
    /**
     * @template STx of object
     * @param class-string<STx> $txClass
     * @return self<STx>
     */
    public static function tx(string $txClass): self
    {
        /** @var self<STx> */
        return new self();
    }

    /**
     * @var list<class-string>
     */
    public array $messageClasses {
        get => array_keys($this->handlers);
    }

    /**
     * @var array<class-string, non-empty-array<string, callable(object, HandlerContext, Tx): void>>
     */
    private array $handlers = [];

    /**
     * @template T of object
     * @param class-string<T> $messageClass
     * @param callable(T, HandlerContext, Tx): void $handler
     * @return self<Tx>
     */
    public function with(string $messageClass, callable $handler, string $qualifier = ''): self
    {
        if (isset($this->handlers[$messageClass][$qualifier])) {
            throw new \LogicException(\sprintf(
                'Handler qualifier "%s" is already registered for message "%s".',
                $qualifier,
                $messageClass,
            ));
        }

        $copy = clone $this;
        $copy->handlers[$messageClass][$qualifier] = $handler;

        return $copy;
    }

    public function findHandlers(string $messageClass, ?string $qualifier = null): array
    {
        $handlers = $this->handlers[$messageClass] ?? [];

        if ($qualifier === null) {
            return array_values($handlers);
        }

        if (isset($handlers[$qualifier])) {
            return [$handlers[$qualifier]];
        }

        return [];
    }
}
