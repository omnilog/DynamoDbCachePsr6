<?php

namespace Omnilog\DynamoDbCache\Converter;

use Psr\Cache\CacheItemInterface;
use Omnilog\DynamoDbCache\DynamoCacheItem;

interface CacheItemConverterInterface
{
    public function supports(CacheItemInterface $cacheItem): bool;

    public function convert(CacheItemInterface $cacheItem): DynamoCacheItem;
}
