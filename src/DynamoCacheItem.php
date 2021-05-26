<?php

namespace Omnilog\DynamoDbCache;

use DateInterval;
use DateTimeInterface;
use Psr\Cache\CacheItemInterface;
use Omnilog\DynamoDbCache\Encoder\CacheItemEncoderInterface;
use Omnilog\DynamoDbCache\Exception\InvalidArgumentException;

final class DynamoCacheItem implements CacheItemInterface
{
    /**
     * @var string
     */
    private $key;

    /**
     * @var bool
     */
    private $isHit;

    /**
     * @var DateTimeInterface|null
     */
    private $expiresAt;

    /**
     * @var string
     */
    private $value;

    /**
     * @var CacheItemEncoderInterface
     */
    private $encoder;

    /**
     * @param string                    $key
     * @param bool                      $isHit
     * @param mixed                     $value
     * @param DateTimeInterface|null    $expiresAt
     * @param CacheItemEncoderInterface $encoder
     *
     * @internal
     */
    public function __construct(
        string $key,
        bool $isHit,
        $value,
        ?DateTimeInterface $expiresAt,
        CacheItemEncoderInterface $encoder
    ) {
        $this->key = $key;
        $this->isHit = $isHit;
        $this->expiresAt = $expiresAt;
        $this->encoder = $encoder;

        $this->set($value);
    }

    public function getKey()
    {
        return $this->key;
    }

    public function get()
    {
        return $this->encoder->decode($this->value);
    }

    public function isHit()
    {
        return $this->isHit && (time() < $this->expiresAt->getTimestamp() || $this->expiresAt === null);
    }

    public function set($value)
    {
        $this->value = $this->encoder->encode($value);

        return $this;
    }

    public function expiresAt($expiration)
    {
        if ($expiration === null) {
            $this->expiresAt = null;
        } elseif ($expiration instanceof DateTimeInterface) {
            $this->expiresAt = $expiration;
        } else {
            throw new InvalidArgumentException('The expiration must be null or instance of ' . DateTimeInterface::class);
        }

        return $this;
    }

    public function expiresAfter($time)
    {
        if ($time === null) {
            $this->expiresAt = null;
        } else {
            if (is_int($time)) {
                $time = new DateInterval("PT{$time}S");
            }
            if (!$time instanceof DateInterval) {
                throw new InvalidArgumentException('The argument must be an int, DateInterval or null');
            }

            $this->expiresAt = new \DateTime('@' . (time() + $time));
        }

        return $this;
    }

    /**
     * @return string
     *
     * @internal
     *
     */
    public function getRaw(): string
    {
        return $this->value;
    }

    /**
     * @return DateTimeInterface|null
     *
     * @internal
     *
     */
    public function getExpiresAt(): ?DateTimeInterface
    {
        return $this->expiresAt;
    }
}
