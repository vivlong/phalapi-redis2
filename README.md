# PhalApi 2.x 的Redis扩展
PhalApi 2.x扩展类库，从 Redis 2.6.12 版本开始， SET 命令的行为可以通过一系列参数来修改，并集成了简单的Redis分布式锁

## 安装和配置
修改项目下的composer.json文件，并添加：  
```
    "vivlong/phalapi-xredis":"dev-master"
```
然后执行```composer update```。  

安装成功后，添加以下配置到/path/to/phalapi/config/app.php文件：  
```php
    /**
     * 扩展类库 - Redis扩展
     */
    'Xredis' => array(
        //Redis链接配置项
        'servers'  => array(
            'host'   => '127.0.0.1',        //Redis服务器地址
            'port'   => '6379',             //Redis端口号
            'prefix' => 'PhalApi_',         //Redis-key前缀
            'auth'   => 'phalapi',          //Redis链接密码
        ),
        // Redis分库对应关系操作时直接使用名称无需使用数字来切换Redis库
        'DB'       => array(
            'developers' => 1,
            'user'       => 2,
            'code'       => 3,
        ),
        //使用阻塞式读取队列时的等待时间单位/秒
        'blocking' => 5,
    ),
```
并根据自己的情况修改填充。  

## 注册
在/path/to/phalapi/config/di.php文件中，注册：  
```php
$di->cache = function () {
    return new \PhalApi\Xredis\Lite(\PhalApi\DI()->config->get("app.xredis.servers"));
};
```

## 使用
常用基础操作(具体API可以查阅代码中src/Lite.php)

```
// 存入永久的键值队
\PhalApi\DI()->redis->set_forever(键名,值,库名);
// 获取永久的键值队
\PhalApi\DI()->redis->get_forever(键名, 库名);
    
// 存入一个有时效性的键值队,默认600秒
\PhalApi\DI()->redis->set_Time(键名,值,有效时间,库名);
// 获取一个有时效性的键值队
\PhalApi\DI()->redis->get_Time(键名, 库名);
    
// 写入队列左边
\PhalApi\DI()->redis->set_Lpush(队列键名,值, 库名);
// 读取队列右边
\PhalApi\DI()->redis->get_lpop(队列键名, 库名);
// 读取队列右边 如果没有读取到阻塞一定时间(阻塞时间或读取配置文件blocking的值)
\PhalApi\DI()->redis->get_Brpop(队列键名,值, 库名);
    
// 删除一个键值队适用于所有
\PhalApi\DI()->redis->del(键名, 库名);
// 自动增长
\PhalApi\DI()->redis->get_incr(键名, 库名);
// 切换DB并且获得操作实例
\PhalApi\DI()->redis->get_redis(键名, 库名);
    
```
