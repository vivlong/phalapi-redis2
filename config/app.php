<?php
/**
 * 请在下面放置任何您需要的应用配置
 */

return array(

    /**
     * 扩展类库 - Redis扩展
     */
    'Xredis' => array(
        //Redis链接配置项
        'servers'  => array(
            'scheme'   => '',
            'path'     => '',
            'ssl'      => ['cafile' => 'private.pem', 'verify_peer' => true],
            'host'     => '127.0.0.1',                                          //Redis服务器地址
            'port'     => '6379',                                               //Redis端口号
            'cluster' => 'predis',
            'prefix'   => '',                                                   //Redis-key前缀
            'password' => '123',                                                //Redis链接密码
        ),
        // Redis分库对应关系
        'DB'       => array(
            'developers' => 1,
            'user'       => 2,
            'code'       => 3,
        ),
        //使用阻塞式读取队列时的等待时间单位/秒
        'blocking' => 5,
    ),
);
