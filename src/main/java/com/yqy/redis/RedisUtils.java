package com.yqy.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author bahsk
 * @createTime 2021-01-30 11:30
 * @description Redis 工具类 建议使用单例模式进行封装
 */
public class RedisUtils {

    private static final String HOST = "47.114.63.55";
    private static final int PORT = 6379;
    private static JedisPool jedisPool = null;

    public static synchronized Jedis getJedis() {

        if(null == jedisPool) {
            GenericObjectPoolConfig config = new JedisPoolConfig();
            config.setMaxIdle(10);
            config.setMaxTotal(100);
            config.setMaxWaitMillis(1000);
            config.setTestOnBorrow(true);

            jedisPool = new JedisPool(config, HOST, PORT);
        }

        return jedisPool.getResource();
    }
}
