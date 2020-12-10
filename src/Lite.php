<?php

namespace PhalApi\Xredis;

use PhalApi\Cache\RedisCache;

/**
 * PhalApi-Redis2 拓展类.
 *
 * @author: vivlong <vivlong <vivlonglz@gmail.com> 2019-08-28
 * @author: 喵了个咪 <wenzhenxi@vip.qq.com> 2017-08-19
 * @Maintenance: Axios <axioscros@aliyun.com> 于 2016-09-01 协助维护
 *
 * 在index.php中注册
 * \PhalApi\DI()->redis = function () {
 *       return new \PhalApi\Redis\Lite(\PhalApi\DI()->config->get("app.xredis.servers"));
 *  };
 *
 * 例子:
 * // 存入永久的键值队
 * \PhalApi\DI()->redis->set_forever(键名,值,库名);
 * // 获取永久的键值队
 * \PhalApi\DI()->redis->get_forever(键名, 库名);
 */
class Lite extends RedisCache
{
    private $db_old;

    /**
     * 重载方法，统一切换DB.
     *
     * @param $name
     * @param $arguments
     *
     * @return mixed
     *
     * @author: Axios <axioscros@aliyun.com> 2016-09-01
     */
    public function __call($name, $arguments)
    {
        $last = count($arguments) - 1;
        $dbname = $arguments[$last];
        $this->switchDB($dbname);
        unset($arguments[$last]);
        $arguments = empty($arguments) ? array() : $arguments;
        if (method_exists($this, $name)) {
            return call_user_func_array(array($this, $name), $arguments);
        } else {
            return call_user_func_array(array($this->redis, $name), $arguments);
        }
    }

    //---------------------------------------------------string类型-------------------------------------------------

    /**
     * 将value 的值赋值给key,生存时间为永久 并根据名称自动切换库.
     */
    protected function set_forever($key, $value)
    {
        return $this->redis->set($this->formatKey($key), $this->formatValue($value));
    }

    /**
     * 获取value 并根据名称自动切换库.
     */
    protected function get_forever($key)
    {
        $value = $this->redis->get($this->formatKey($key));

        return $value !== false ? $this->unformatValue($value) : null;
    }

    /**
     * 存入一个有实效性的键值队
     */
    protected function set_time($key, $value, $expire = 600)
    {
        return $this->redis->setex($this->formatKey($key), $expire, $this->formatValue($value));
    }

    /**
     * 更新具有有效时间key的value，不重置有效时间.
     */
    protected function save_time($key, $value)
    {
        if ($this->get_exists($key)) {
            $ttl = $this->get_time_ttl($key);

            return $this->set_time($key, $value, $ttl);
        }

        return null;
    }

    /**
     * 统一get/set方法,对于set_Time使用get_Time.
     */
    protected function get_time($key)
    {
        $value = $this->redis->get($this->formatKey($key));

        return $value !== false ? $this->unformatValue($value) : null;
    }

    /**
     * 得到一个key的生存时间.
     */
    protected function get_time_ttl($key)
    {
        $value = $this->redis->ttl($this->formatKey($key));

        return $value !== false ? $value : null;
    }

    /**
     * 批量插入k-v,请求的v需要是一个数组 如下格式
     * array('key0' => 'value0', 'key1' => 'value1').
     */
    protected function set_list($value)
    {
        $data = array();
        foreach ($value as $k => $v) {
            $data[$this->formatKey($k)] = $this->formatValue($v);
        }

        return $this->redis->mset($data);
    }

    /**
     * 批量获取k-v,请求的k需要是一个数组.
     */
    protected function get_list($key)
    {
        $data = array();
        foreach ($key as $k => $v) {
            $data[] = $this->formatKey($v);
        }
        $rs = $this->redis->mget($data);
        foreach ($rs as $k => $v) {
            $rs[$k] = $this->unformatValue($v);
        }

        return $rs;
    }

    /**
     * 判断key是否存在。存在 true 不在 false.
     */
    protected function get_exists($key)
    {
        return $this->redis->exists($this->formatKey($key));
    }

    /**
     * 返回原来key中的值，并将value写入key.
     */
    protected function get_getSet($key, $value)
    {
        $value = $this->redis->getSet($this->formatKey($key), $this->formatValue($value));

        return $value !== false ? $this->unformatValue($value) : null;
    }

    /**
     * string，名称为key的string的值在后面加上value.
     */
    protected function set_append($key, $value)
    {
        return $this->redis->append($this->formatKey($key), $this->formatValue($value));
    }

    /**
     * 返回原来key中的值，并将value写入key.
     */
    protected function get_strlen($key)
    {
        return $this->redis->strlen($this->formatKey($key));
    }

    /**
     * 自动增长
     * value为自增长的值默认1.
     */
    protected function get_incr($key, $value = 1)
    {
        return $this->redis->incr($this->formatKey($key), $value);
    }

    /**
     * 自动减少
     * value为自减少的值默认1.
     */
    protected function get_decr($key, $value = 1)
    {
        return $this->redis->decr($this->formatKey($key), $value);
    }

    //------------------------------------------------List类型-------------------------------------------------

    /**
     * 写入队列左边 并根据名称自动切换库.
     */
    protected function set_lPush($key, $value)
    {
        return $this->redis->lPush($this->formatKey($key), $this->formatValue($value));
    }

    /**
     * 写入队列左边 如果value已经存在，则不添加 并根据名称自动切换库.
     */
    protected function set_lPushx($key, $value)
    {
        return $this->redis->lPushx($this->formatKey($key), $this->formatValue($value));
    }

    /**
     * 写入队列右边 并根据名称自动切换库.
     */
    protected function set_rPush($key, $value)
    {
        return $this->redis->rPush($this->formatKey($key), $this->formatValue($value));
    }

    /**
     * 写入队列右边 如果value已经存在，则不添加 并根据名称自动切换库.
     */
    protected function set_rPushx($key, $value)
    {
        return $this->redis->rPushx($this->formatKey($key), $this->formatValue($value));
    }

    /**
     * 读取队列左边.
     */
    protected function get_lPop($key)
    {
        $value = $this->redis->lPop($this->formatKey($key));

        return $value != false ? $this->unformatValue($value) : null;
    }

    /**
     * 读取队列右边.
     */
    protected function get_rPop($key)
    {
        $value = $this->redis->rPop($this->formatKey($key));

        return $value != false ? $this->unformatValue($value) : null;
    }

    /**
     * 读取队列左边 如果没有读取到阻塞一定时间 并根据名称自动切换库.
     */
    protected function get_blPop($key)
    {
        $value = $this->redis->blPop($this->formatKey($key), \PhalApi\DI()->config->get('app.redis.blocking'));

        return $value != false ? $this->unformatValue($value[1]) : null;
    }

    /**
     * 读取队列右边 如果没有读取到阻塞一定时间 并根据名称自动切换库.
     */
    protected function get_brPop($key)
    {
        $value = $this->redis->brPop($this->formatKey($key), \PhalApi\DI()->config->get('app.redis.blocking'));

        return $value != false ? $this->unformatValue($value[1]) : null;
    }

    /**
     * 名称为key的list有多少个元素.
     */
    protected function get_lSize($key)
    {
        return $this->redis->lSize($this->formatKey($key));
    }

    /**
     * 返回名称为key的list中指定位置的元素.
     */
    protected function set_lSet($key, $index, $value)
    {
        return $this->redis->lSet($this->formatKey($key), $index, $this->formatValue($value));
    }

    /**
     * 返回名称为key的list中指定位置的元素.
     */
    protected function get_lGet($key, $index)
    {
        $value = $this->redis->lGet($this->formatKey($key), $index);

        return $value != false ? $this->unformatValue($value[1]) : null;
    }

    /**
     * 返回名称为key的list中start至end之间的元素（end为 -1 ，返回所有）.
     */
    protected function get_lRange($key, $start, $end)
    {
        $rs = $this->redis->lRange($this->formatKey($key), $start, $end);
        foreach ($rs as $k => $v) {
            $rs[$k] = $this->unformatValue($v);
        }

        return $rs;
    }

    /**
     * 截取名称为key的list，保留start至end之间的元素.
     */
    protected function get_lTrim($key, $start, $end)
    {
        $rs = $this->redis->lTrim($this->formatKey($key), $start, $end);
        foreach ($rs as $k => $v) {
            $rs[$k] = $this->unformatValue($v);
        }

        return $rs;
    }

    //未实现 lRem lInsert  rpoplpush
    //----------------------------------------------------set类型---------------------------------------------------
    //----------------------------------------------------zset类型---------------------------------------------------
    //----------------------------------------------------Hash类型---------------------------------------------------

    //----------------------------------------------------通用方法---------------------------------------------------

    /**
     * 永久计数器,回调当前计数.
     *
     * @author Axios <axioscros@aliyun.com>
     */
    public function counter_forever($key, $dbname = 0)
    {
        $this->switchDB($dbname);
        if ($this->get_exists($key)) {
            $count = $this->get_forever($key);
            ++$count;
            $this->set_forever($key, $count);
        } else {
            $count = 1;
            $this->set_forever($key, $count);
        }

        return $count;
    }

    /**
     * 创建具有有效时间的计数器,回调当前计数,单位毫秒ms.
     *
     * @author Axios <axioscros@aliyun.com>
     */
    public function counter_time_create($key, $expire = 1000, $dbname = 0)
    {
        $this->switchDB($dbname);
        $count = 1;
        $this->set_time($key, $count, $expire);
        $this->redis->pSetEx($this->formatKey($key), $expire, $this->formatValue($count));

        return $count;
    }

    /**
     * 更新具有有效时间的计数器,回调当前计数.
     *
     * @author Axios <axioscros@aliyun.com>
     */
    public function counter_time_update($key, $dbname = 0)
    {
        $this->switchDB($dbname);
        if ($this->get_exists($key)) {
            $count = $this->get_time($key);
            ++$count;
            $expire = $this->redis->pttl($this->formatKey($key));
            $this->set_time($key, $count, $expire);

            return $count;
        }

        return false;
    }

    /**
     * 设定一个key的活动时间（s）.
     */
    protected function setTimeout($key, $time = 600)
    {
        return $this->redis->setTimeout($key, $time);
    }

    /**
     * 返回key的类型值
     */
    protected function type($key)
    {
        return $this->redis->type($key);
    }

    /**
     * key存活到一个unix时间戳时间.
     */
    protected function expireAt($key, $time = 600)
    {
        return $this->redis->expireAt($key, $time);
    }

    /**
     * 随机返回key空间的一个key.
     */
    public function randomKey()
    {
        return $this->redis->randomKey();
    }

    /**
     * 返回满足给定pattern的所有key.
     */
    protected function keys($key, $pattern)
    {
        return $this->redis->keys($key, $pattern);
    }

    /**
     * 查看现在数据库有多少key.
     */
    protected function dbSize()
    {
        return $this->redis->dbSize();
    }

    /**
     * 转移一个key到另外一个数据库.
     */
    protected function move($key, $db)
    {
        $arr = \PhalApi\DI()->config->get('app.redis.DB');
        $rs = isset($arr[$db]) ? $arr[$db] : $db;

        return $this->redis->move($key, $rs);
    }

    /**
     * 给key重命名.
     */
    protected function rename($key, $key2)
    {
        return $this->redis->rename($key, $key2);
    }

    /**
     * 给key重命名 如果重新命名的名字已经存在，不会替换成功
     */
    protected function renameNx($key, $key2)
    {
        return $this->redis->renameNx($key, $key2);
    }

    /**
     * 删除键值 并根据名称自动切换库(对所有通用).
     */
    protected function del($key)
    {
        return $this->redis->del($this->formatKey($key));
    }

    /**
     * 返回redis的版本信息等详情.
     */
    public function info()
    {
        return $this->redis->info();
    }

    /**
     * 切换DB并且获得操作实例.
     */
    public function get_redis()
    {
        return $this->redis;
    }

    /**
     * 查看连接状态
     */
    public function ping()
    {
        return $this->redis->ping();
    }

    /**
     * 内部切换Redis-DB 如果已经在某个DB上则不再切换.
     */
    protected function switchDB($name)
    {
        $arr = \PhalApi\DI()->config->get('app.redis.DB');
        if (is_int($name)) {
            $db = $name;
        } else {
            $db = isset($arr[$name]) ? $arr[$name] : 0;
        }
        if ($this->db_old != $db) {
            $this->redis->select($db);
            $this->db_old = $db;
        }
    }

    //-------------------------------------------------------谨慎使用------------------------------------------------

    /**
     * 清空当前数据库.
     */
    protected function flushDB()
    {
        return $this->redis->flushDB();
    }

    /**
     * 清空所有数据库.
     */
    public function flushAll()
    {
        return $this->redis->flushAll();
    }

    /**
     * 选择从服务器.
     */
    public function slaveof($host, $port)
    {
        return $this->redis->slaveof($host, $port);
    }

    /**
     * 将数据同步保存到磁盘.
     */
    public function save()
    {
        return $this->redis->save();
    }

    /**
     * 将数据异步保存到磁盘.
     */
    public function bgsave()
    {
        return $this->redis->bgsave();
    }

    /**
     * 返回上次成功将数据保存到磁盘的Unix时戳.
     */
    public function lastSave()
    {
        return $this->redis->lastSave();
    }

    /**
     * 使用aof来进行数据库持久化.
     */
    protected function bgrewriteaof()
    {
        return $this->redis->bgrewriteaof();
    }

    /**
     *  从 Redis 2.6.12 版本开始， SET 命令的行为可以通过一系列参数来修改
     *  在 Redis 2.6.12 版本以前， SET 命令总是返回 OK
     *  从 Redis 2.6.12 版本开始， SET 在设置操作成功完成时，才返回 OK
     *  如果设置了 NX 或者 XX ，但因为条件没达到而造成设置操作未执行，那么命令返回空批量回复（NULL Bulk Reply）.
     */
    protected function set_params($key, $value, $params)
    {
        return $this->redis->set($this->formatKey($key), $this->formatValue($value), $params);
    }

    /**
     * 一种在 Redis 中实现锁的简单方法.
     *
     * 可以通过以下修改，让这个锁实现更健壮：
     * -- 1、不使用固定的字符串作为键的值，而是设置一个不可猜测（non-guessable）的长随机字符串，作为口令串（token）。
     * -- 2、不使用 DEL 命令来释放锁，而是发送一个 Lua 脚本，这个脚本只在客户端传入的值和键的口令串相匹配时，才对键进行删除。
     * 这两个改动可以防止持有过期锁的客户端误删现有锁的情况出现。
     * 以下是一个简单的解锁脚本示例：
     * -- if redis.call("get",KEYS[1]) == ARGV[1]
     * -- then
     * --     return redis.call("del",KEYS[1])
     * -- else
     * --     return 0
     * -- end
     * 这个脚本可以通过 EVAL ...script... 1 resource-name token-value 命令来调用。
     *
     * 获得锁,如果锁被占用,阻塞,直到获得锁或者超时。
     * -- 1、如果 $timeout 参数为 0,则立即返回锁。
     * -- 2、建议 timeout 设置为 0,避免 redis 因为阻塞导致性能下降。请根据实际需求进行设置。
     *
     * @param string $key        缓存KEY。
     * @param int    $timeout    取锁超时时间。单位(秒)。等于0,如果当前锁被占用,则立即返回失败。如果大于0,则反复尝试获取锁直到达到该超时时间。
     * @param int    $lockSecond 锁定时间。单位(秒)。
     * @param int    $sleep      取锁间隔时间。单位(微秒)。当锁为占用状态时。每隔多久尝试去取锁。默认 0.1 秒一次取锁。
     *
     * @return bool 成功:true、失败:false
     */
    protected function lock($key, $timeout = 0, $lockSecond = null, $sleep = 100000)
    {
        $di = \PhalApi\DI();
        if (empty($lockSecond)) {
            $lockSecond = $di->config->get('app.redis.blocking');
        }
        $start = $this->getMicroTime();
        do {
            // [1] 锁的 KEY 不存在时设置其值并把过期时间设置为指定的时间。锁的值并不重要。重要的是利用 Redis 的特性。
            $acquired = $this->set_params('LOCK_'.$key, 1, ['NX', 'EX' => $lockSecond]);
            if ($acquired) {
                break;
            }
            if ($timeout === 0) {
                break;
            }
            usleep($sleep);
        } while (!is_numeric($timeout) || ($this->getMicroTime()) < ($start + ($timeout * 1000000)));

        return $acquired ? true : false;
    }

    /**
     * 释放锁
     *
     * @param mixed $key 被加锁的KEY。
     */
    protected function release($key)
    {
        $this->del('LOCK_'.$key);
    }

    /**
     * 获取当前微秒。
     *
     * @return bigint
     */
    protected static function getMicroTime()
    {
        return bcmul(microtime(true), 1000000);
    }
}
