package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * 基于redis的全局唯一id生成器
 */
@Component
public class RedisIdWorker {

    //2022年对应的秒数，也就是开始的时间戳
    private static final long BEGIN_TIMESTAMP = 1640995200L;
    //序列位32位
    private static final long COUNT_BITS = 32 ;
    private StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    //keyPrefix前缀用于区分不同的业务
    public long nextId(String keyPrefix){

        //1、生成时间戳，当前时间戳减去开始时间戳
        long timeStamp = (LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)) - BEGIN_TIMESTAMP;
        //2、生成序列号,利用redis的自增指令
        //2.1、获取当前日期，精确到天
        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);
        //拼接然后返回
        //使用位运算，
        return timeStamp << COUNT_BITS | count;
    }
}
