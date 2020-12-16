<?php

namespace PhalApi\Xredis;

use PhalApi\Cache;

/**
 * RedisCache.
 */
class RedisCache implements Cache
{
    protected $redis;

    protected $prefix;

    public function __construct($config)
    {
        $port = isset($config['port']) ? intval($config['port']) : 6379;
        $timeout = isset($config['timeout']) ? intval($config['timeout']) : 300;
        $this->redis = new \Predis\Client([
            'scheme' => 'tcp',
            'host' => $config['host'],
            'port' => $port,
            'password' => $config['password'],
        ], ['prefix' => $config['prefix']]);
        $this->redis->connect();
        // 选择
        $dbIndex = isset($config['db']) ? intval($config['db']) : 0;
        $this->redis->select($dbIndex);
        $this->prefix = isset($config['prefix']) ? $config['prefix'] : 'phalapi:';
    }

    /**
     * 将value 的值赋值给key,生存时间为expire秒.
     */
    public function set($key, $value, $expire = 600)
    {
        $this->redis->setex($this->formatKey($key), $expire, $this->formatValue($value));
    }

    public function get($key)
    {
        $value = $this->redis->get($this->formatKey($key));

        return false !== $value ? $this->unformatValue($value) : null;
    }

    public function delete($key)
    {
        return $this->redis->delete($this->formatKey($key));
    }

    /**
     * 拉取缓存，拉取后同时删除缓存.
     *
     * @return minxed|null 缓存不存在时返回NULL
     */
    public function pull($key)
    {
        $value = $this->get($key);
        $this->delete($key);

        return $value;
    }

    /**
     * 检测是否存在key,若不存在则赋值value.
     */
    public function setnx($key, $value)
    {
        return $this->redis->setnx($this->formatKey($key), $this->formatValue($value));
    }

    public function lPush($key, $value)
    {
        return $this->redis->lPush($this->formatKey($key), $this->formatValue($value));
    }

    public function rPush($key, $value)
    {
        return $this->redis->rPush($this->formatKey($key), $this->formatValue($value));
    }

    public function lPop($key)
    {
        $value = $this->redis->lPop($this->formatKey($key));

        return false !== $value ? $this->unformatValue($value) : null;
    }

    public function rPop($key)
    {
        $value = $this->redis->rPop($this->formatKey($key));

        return false !== $value ? $this->unformatValue($value) : null;
    }

    protected function formatKey($key)
    {
        return $this->prefix.$key;
    }

    protected function formatValue($value)
    {
        return @serialize($value);
    }

    protected function unformatValue($value)
    {
        return @unserialize($value);
    }

    /**
     * 获取Redis实例，当封装的方法未能满足时，可调用此接口获取Reids实例进行操作.
     */
    public function getRedis()
    {
        return $this->redis;
    }
}
