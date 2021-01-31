package com.yqy.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author bahsk
 * @createTime 2021-01-29 21:39
 * @description
 */
public class RedisApp {

    private String host = "47.114.63.55";
    private int port = 6379;

    private Jedis jedis;

    @Before
    public void setUp() throws Exception {
        jedis = new Jedis(host, port);
    }


    @Test
    public void testSetInfo() {
        jedis.set("name", "yqy");
        Assert.assertEquals("yqy", jedis.get("name"));

    }

    @Test
    public void testGetInfo() {
        System.out.println(jedis.get("name"));
    }

    @Test
    public void test02() {
        GenericObjectPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(10); //最大空闲连接数
        config.setMaxTotal(100); //最大活动连接数
        config.setMaxWaitMillis(1000); //从连接池获取一个连接时最大等待时长
        config.setTestOnBorrow(true); //从连接池获取一个连接时,取出是否检测

        JedisPool jedisPool = new JedisPool(config, host, port);
        Jedis jedis = jedisPool.getResource();
        Assert.assertEquals("yqy", jedis.get("name"));

    }

    @Test
    public void testGetJedis(){
        Jedis jedis = RedisUtils.getJedis();
        Assert.assertEquals("100", jedis.get("age"));
    }



    @After
    public void tearDown() throws Exception {
        jedis.close();
    }
}
