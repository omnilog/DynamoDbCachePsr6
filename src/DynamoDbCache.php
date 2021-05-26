<?php

namespace Omnilog\DynamoDbCache;

use AsyncAws\DynamoDb\DynamoDbClient;
use AsyncAws\DynamoDb\Enum\ReturnValue;
use AsyncAws\DynamoDb\Input\BatchGetItemInput;
use AsyncAws\DynamoDb\Input\GetItemInput;
use AsyncAws\DynamoDb\Input\PutItemInput;
use AsyncAws\DynamoDb\ValueObject\AttributeValue;
use DateInterval;
use Psr\Cache\CacheItemInterface;
use Psr\Cache\CacheItemPoolInterface;
use Psr\SimpleCache\CacheInterface;
use Omnilog\DynamoDbCache\Converter\CacheItemConverterRegistry;
use Omnilog\DynamoDbCache\Converter\DefaultCacheItemConverter;
use Omnilog\DynamoDbCache\Encoder\CacheItemEncoderInterface;
use Omnilog\DynamoDbCache\Encoder\SerializeItemEncoder;
use Omnilog\DynamoDbCache\Exception\InvalidArgumentException;

final class DynamoDbCache implements CacheItemPoolInterface, CacheInterface
{
    private const RESERVED_CHARACTERS = '{}()/\@:';

    /**
     * @var string
     */
    private $tableName;

    /**
     * @var DynamoDbClient
     */
    private $client;

    /**
     * @var string
     */
    private $primaryField;

    /**
     * @var string
     */
    private $ttlField;

    /**
     * @var string
     */
    private $valueField;

    /**
     * @var DynamoCacheItem[]
     */
    private $deferred = [];

    /**
     * @var CacheItemConverterRegistry
     */
    private $converter;

    /**
     * @var CacheItemEncoderInterface
     */
    private $encoder;

    /**
     * @var string|null
     */
    private $prefix;

    public function __construct(
        string $tableName,
        DynamoDbClient $client,
        string $primaryField = 'id',
        string $ttlField = 'ttl',
        string $valueField = 'value',
        ?CacheItemConverterRegistry $converter = null,
        ?CacheItemEncoderInterface $encoder = null,
        ?string $prefix = null
    ) {
        $this->tableName = $tableName;
        $this->client = $client;
        $this->primaryField = $primaryField;
        $this->ttlField = $ttlField;
        $this->valueField = $valueField;

        if ($encoder === null) {
            $encoder = new SerializeItemEncoder();
        }
        $this->encoder = $encoder;

        if ($converter === null) {
            $converter = new CacheItemConverterRegistry(
                new DefaultCacheItemConverter($this->encoder)
            );
        }
        $this->converter = $converter;
        $this->prefix = $prefix;
    }

    /**
     * @param string $key
     *
     * @throws InvalidArgumentException
     *
     * @return DynamoCacheItem
     */
    public function getItem($key)
    {
        if ($exception = $this->getExceptionForInvalidKey($this->getKey($key))) {
            throw $exception;
        }

//        try {

        $item = $this->client->getItem(new GetItemInput([
            'TableName' => $this->tableName,
            'ConsistentRead' => true,
            'Key' => [
                $this->primaryField => new AttributeValue(['S' => $this->getKey($key)]),
            ],
        ]))->getItem();

        $data = isset($item[$this->valueField]) && $item[$this->valueField]->getS() ? $item[$this->valueField]->getS() : null;
        $ttl = isset($item[$this->ttlField]) && $item[$this->ttlField]->getN() ? $item[$this->ttlField]->getN() : null;

        return new DynamoCacheItem(
            $this->getKey($key),
            $data !== null,
            $data !== null ? $this->encoder->decode($data) : null,
            $ttl!== null ? (new \DateTime())->setTimestamp((int) $ttl) : null,
            $this->encoder
        );
//        } catch (DynamoDbException $e) {
//            if ($e->getAwsErrorCode() === 'ResourceNotFoundException') {
//                return new DynamoCacheItem(
//                    $this->getKey($key),
//                    false,
//                    null,
//                    null,
//                    $this->encoder
//                );
//            }
//            throw $e;
//        }
    }

    /**
     * @param string[] $keys
     *
     * @throws InvalidArgumentException
     *
     * @return DynamoCacheItem[]
     */
    public function getItems(array $keys = [])
    {
        $keys = array_map(function ($key) {
            if ($exception = $this->getExceptionForInvalidKey($this->getKey($key))) {
                throw $exception;
            }

            return $this->getKey($key);
        }, $keys);

        $response = $this->client->batchGetItem(new BatchGetItemInput([
            'RequestItems' => [
                $this->tableName => [
                    'Keys' => array_map(function ($key) {
                        return [
                            $this->primaryField => [
                                'S' => $key,
                            ],
                        ];
                    }, $keys),
                ],
            ]
        ]));

//        $response = $this->client->batchGetItem([
//            'RequestItems' => [
//                $this->tableName => [
//                    'Keys' => array_map(function ($key) {
//                        return [
//                            $this->primaryField => [
//                                'S' => $key,
//                            ],
//                        ];
//                    }, $keys),
//                ],
//            ],
//        ]);

        $result = [];
        foreach ($response->getResponses()[$this->tableName] as $item) {
            $result[] = new DynamoCacheItem(
                $item[$this->primaryField]['S'],
                true,
                $this->encoder->decode($item[$this->valueField]['S']),
                ($item[$this->ttlField]['N'] ?? null) !== null
                    ? (new \DateTime())->setTimestamp((int) $item[$this->ttlField]['N'])
                    : null,
                $this->encoder
            );
        }
        foreach ($response->getUnprocessedKeys()[$this->tableName] ?? [] as $item) {
            $unprocessedKeys = $item['Keys'];
            foreach ($unprocessedKeys as $key) {
                $result[] = new DynamoCacheItem(
                    $key['S'],
                    false,
                    null,
                    null,
                    $this->encoder
                );
            }
        }

        if (count($result) !== count($keys)) {
            $processedKeys = array_map(function (DynamoCacheItem $cacheItem) {
                return $cacheItem->getKey();
            }, $result);
            $unprocessed = array_diff($keys, $processedKeys);
            foreach ($unprocessed as $unprocessedKey) {
                $result[] = new DynamoCacheItem(
                    $unprocessedKey,
                    false,
                    null,
                    null,
                    $this->encoder
                );
            }
        }

        return $result;
    }

    /**
     * @param string $key
     *
     * @throws InvalidArgumentException
     *
     * @return bool
     */
    public function hasItem($key)
    {
        return $this->getItem($key)->isHit();
    }

    /**
     * @return false
     */
    public function clear()
    {
        return false;
    }

    /**
     * @param string|DynamoCacheItem $key
     *
     * @throws InvalidArgumentException
     *
     * @return bool
     */
    public function deleteItem($key)
    {
        if ($key instanceof DynamoCacheItem) {
            $key = $key->getKey();
        } else {
            $key = $this->getKey($key);
        }

        if ($exception = $this->getExceptionForInvalidKey($key)) {
            throw $exception;
        }

//        try {
        $response = $this->client->deleteItem([
            'Key' => [
                $this->primaryField => [
                    'S' => $key,
                ],
            ],
            'TableName' => $this->tableName,
            'ReturnValues' => ReturnValue::ALL_OLD,
        ]);

        return count($response->getAttributes()) > 0;
//        } catch (DynamoDbException $e) {
//            return false;
//        }
    }

    /**
     * @param string[] $keys
     *
     * @throws InvalidArgumentException
     *
     * @return bool
     */
    public function deleteItems(array $keys)
    {
        $keys = array_map(function ($key) {
            if ($exception = $this->getExceptionForInvalidKey($this->getKey($key))) {
                throw $exception;
            }

            return $this->getKey($key);
        }, $keys);

//        try {
        $this->client->batchWriteItem([
            'RequestItems' => [
                $this->tableName => array_map(function ($key) {
                    return [
                        'DeleteRequest' => [
                            'Key' => [
                                $this->primaryField => [
                                    'S' => $key,
                                ],
                            ],
                        ],
                    ];
                }, $keys),
            ],
        ]);

        return true;
//        } catch (DynamoDbException $e) {
//            return false;
//        }
    }

    /**
     * @param CacheItemInterface $item
     *
     * @throws InvalidArgumentException
     *
     * @return bool
     */
    public function save(CacheItemInterface $item)
    {
        $item = $this->converter->convert($item);
        if ($exception = $this->getExceptionForInvalidKey($item->getKey())) {
            throw $exception;
        }

//        try {
        $data = [
            'Item' => [
                $this->primaryField => [
                    'S' => $item->getKey(),
                ],
                $this->valueField => [
                    'S' => $item->getRaw(),
                ],
            ],
            'TableName' => $this->tableName,
        ];

        if ($expiresAt = $item->getExpiresAt()) {
            $data['Item'][$this->ttlField]['N'] = (string) $expiresAt->getTimestamp();
        }

        $result =   $this->client->putItem(new PutItemInput($data));

        return $result->resolve();
//        } catch (DynamoDbException $e) {
//            return false;
//        }
    }

    /**
     * @param CacheItemInterface $item
     *
     * @throws InvalidArgumentException
     *
     * @return bool
     */
    public function saveDeferred(CacheItemInterface $item)
    {
        if ($exception = $this->getExceptionForInvalidKey($item->getKey())) {
            throw $exception;
        }
        $item = $this->converter->convert($item);

        $this->deferred[] = $item;

        return true;
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return bool
     */
    public function commit()
    {
        $result = true;
        foreach ($this->deferred as $key => $item) {
            $itemResult = $this->save($item);
            $result = $itemResult && $result;

            if ($itemResult) {
                unset($this->deferred[$key]);
            }
        }

        return $result;
    }

    /**
     * @param string $key
     * @param mixed  $default
     *
     * @throws InvalidArgumentException
     *
     * @return mixed
     */
    public function get($key, $default = null)
    {
        $item = $this->getItem($key);
        if (!$item->isHit()) {
            return $default;
        }

        return $item->get();
    }

    /**
     * @param string                $key
     * @param mixed                 $value
     * @param int|DateInterval|null $ttl
     *
     * @throws InvalidArgumentException
     *
     * @return bool
     */
    public function set($key, $value, $ttl = null)
    {
        $item = $this->getItem($key);
        if ($ttl !== null) {
            $item->expiresAfter($ttl);
        }
        $item->set($value);

        return $this->save($item);
    }

    /**
     * @param string $key
     *
     * @throws InvalidArgumentException
     *
     * @return bool
     */
    public function delete($key)
    {
        return $this->deleteItem($key);
    }

    /**
     * @param iterable<string> $keys
     * @param mixed            $default
     *
     * @throws InvalidArgumentException
     *
     * @return mixed[]
     */
    public function getMultiple($keys, $default = null)
    {
        $result = array_combine(
            $this->iterableToArray($keys),
            array_map(function (DynamoCacheItem $item) use ($default) {
                if ($item->isHit()) {
                    return $item->get();
                }

                return $default;
            }, $this->getItems($this->iterableToArray($keys)))
        );
        assert(is_array($result));

        return $result;
    }

    /**
     * @param iterable<string,mixed> $values
     * @param int|DateInterval|null  $ttl
     *
     * @throws InvalidArgumentException
     *
     * @return bool
     */
    public function setMultiple($values, $ttl = null)
    {
        foreach ($values as $key => $value) {
            $item = $this->getItem($key);
            $item->set($value);
            if ($ttl !== null) {
                $item->expiresAfter($ttl);
            }
            $this->saveDeferred($item);
        }

        return $this->commit();
    }

    /**
     * @param iterable<string> $keys
     *
     * @throws InvalidArgumentException
     *
     * @return bool
     */
    public function deleteMultiple($keys)
    {
        return $this->deleteItems($this->iterableToArray($keys));
    }

    /**
     * @param string $key
     *
     * @throws InvalidArgumentException
     *
     * @return bool
     */
    public function has($key)
    {
        return $this->hasItem($key);
    }

    private function getExceptionForInvalidKey(string $key): ?InvalidArgumentException
    {
        if (strpbrk($key, self::RESERVED_CHARACTERS) !== false) {
            return new InvalidArgumentException(
                sprintf(
                    "The key '%s' cannot contain any of the reserved characters: '%s'",
                    $key,
                    self::RESERVED_CHARACTERS
                )
            );
        }

        return null;
    }

    /**
     * @param iterable<mixed,mixed> $iterable
     *
     * @return array<mixed,mixed>
     */
    private function iterableToArray(iterable $iterable): array
    {
        if (is_array($iterable)) {
            return $iterable;
        } else {
            /** @noinspection PhpParamsInspection */
            return iterator_to_array($iterable);
        }
    }

    private function getKey(string $key): string
    {
        if ($this->prefix !== null) {
            return $this->prefix . $key;
        }

        return $key;
    }
}
