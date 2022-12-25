package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    //设置过期时间
    public void setKeyTTL(String key, Object value, Long time, TimeUnit unit){
        String jsonStr = JSONUtil.toJsonStr(value);
        stringRedisTemplate.opsForValue().set(key,jsonStr,time,unit);

    }
    //设置逻辑过期时间
    public void setKeyLogicExpire(String key,Object value ,Long time, TimeUnit unit){
        //原有数据增加逻辑过期时间属性，封装成一个新对象
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        String jsonStr = JSONUtil.toJsonStr(redisData);
        stringRedisTemplate.opsForValue().set(key,jsonStr);
    }

    //解决缓存穿透
    public  <R,ID> R  queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallBack,Long time, TimeUnit unit){
        //1、根据key查询redis缓存
        String Json = stringRedisTemplate.opsForValue().get(keyPrefix +id);
        //2、如果存在，返回
        if (StrUtil.isNotBlank(Json)) {
            return JSONUtil.toBean(Json, type);
        }
        //多加一个判断是否为空值的可能，如果为空值返回错误
        if(Json != null){
            return null;
        }
        //3、如果不存在，就去查询数据库
        R r = dbFallBack.apply(id);
        //4、数据库中不存在
        if (r==null) {
            //空值写入redis
            stringRedisTemplate.opsForValue().set(keyPrefix + id,"" , CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回空对象
            return null;
        }
        //5、数据库中存在，把数据写入redis，并返回
        this.setKeyTTL(keyPrefix + id,r,time, unit);
        return r;
    }


    //线程池
    private static final ExecutorService CACHE_REBULID_EXECUTOR = Executors.newFixedThreadPool(10);

    //逻辑过期解决缓存击穿问题
    public  <R,ID> R queryWithLogicExpire(String keyPrefix,ID id,Class<R> type,Function<ID,R> dbFallBack,Long time,TimeUnit unit){
        //1、根据id查询redis缓存
        String Json = stringRedisTemplate.opsForValue().get(keyPrefix + id);
        //2、如果不存在，返回空
        if (StrUtil.isBlank(Json)) {
            return null;
        }
        //3、如果存在，就要判断逻辑过期时间
        RedisData redisData = JSONUtil.toBean(Json, RedisData.class);
        //获取逻辑过期时间
        LocalDateTime expireTime = redisData.getExpireTime();
        //获取要查询的shop数据
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        //判断如果逻辑过期时间未过期，直接返回查到的数据
        if(expireTime.isAfter(LocalDateTime.now())){
            return r;
        }
        //如果已过期，就要进行缓存重建
        String lockKey = LOCK_SHOP_KEY +id;
        //获取锁
        boolean isLock = tryLock(lockKey);
        if (isLock) {
            // 成功获取锁，开启独立线程
            CACHE_REBULID_EXECUTOR.submit(()->{
                try {
                    //重建缓存数据
                    //查询数据库
                    R apply = dbFallBack.apply(id);
                    //写入缓存
                    this.setKeyLogicExpire(keyPrefix + id,apply,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    deleteLock(lockKey);
                }
            });
        }
        //返回
        return r;
    }


    //定义获取锁的方法
    private boolean tryLock(String key){
        //使用的是redis的原有命令setnx
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10L, TimeUnit.MINUTES);
        return BooleanUtil.isTrue(flag);
    }
    //定义删除锁的方法
    private void deleteLock(String key){
        stringRedisTemplate.delete(key);
    }



}
