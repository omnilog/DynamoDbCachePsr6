<?php

namespace Omnilog\DynamoDbCache\Converter;

use Psr\Cache\CacheItemInterface;
use Omnilog\DynamoDbCache\DynamoCacheItem;
use Omnilog\DynamoDbCache\Encoder\CacheItemEncoderInterface;
use Omnilog\DynamoDbCache\Encoder\SerializeItemEncoder;

final class DefaultCacheItemConverter implements CacheItemConverterInterface
{
    /**
     * @var CacheItemEncoderInterface
     */
    private $encoder;

    public function __construct(?CacheItemEncoderInterface $encoder = null)
    {
        if ($encoder === null) {
            $encoder = new SerializeItemEncoder();
        }
        $this->encoder = $encoder;
    }

    public function supports(CacheItemInterface $cacheItem): bool
    {
        return true;
    }

    public function convert(CacheItemInterface $cacheItem): DynamoCacheItem
    {
        if ($cacheItem instanceof DynamoCacheItem) {
            return $cacheItem;
        }

        // the expiration date may be lost in the process
        $cacheItem = new DynamoCacheItem(
            $cacheItem->getKey(),
            $cacheItem->isHit(),
            $cacheItem->get(),
            null,
            $this->encoder
        );

        return $cacheItem;
    }
}
