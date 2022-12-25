package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;


public class SimpleRedisLock implements ILock{

    private String name;
    private StringRedisTemplate stringRedisTemplate;
    private static final String KEY_PREFIX = "lock:";
    //用UUID生成线程标识的前缀
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) +"-";
    //脚本对象
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    //静态加载初始化脚本对象
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        //获取当前线程的标识作为锁的值存入redis
        String  threadId = ID_PREFIX +Thread.currentThread().getId();
        //通过redis的setnx，来实现锁，set key  threadid  ex time  nx  过期时间是防止服务宕机没有释放锁
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(KEY_PREFIX +name,threadId ,timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }


    //基于lua脚本，来实现释放锁的原子操作
    @Override
    public void unlock() {
        //调用lua脚本
        stringRedisTemplate.execute(UNLOCK_SCRIPT, Collections.singletonList(KEY_PREFIX +name),ID_PREFIX +Thread.currentThread().getId());
    }

 /*   @Override
    public void unlock() {
//        释放锁
        //先获取线程标识，然后再删除自己的锁
        String  threadId = ID_PREFIX +Thread.currentThread().getId();
        //取出设置锁时在value中设置的值
        String lockValue = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
        if (threadId.equals(lockValue)) {
            //是自己的锁，可以删掉
            stringRedisTemplate.delete(KEY_PREFIX +name);
        }
    } */
}
