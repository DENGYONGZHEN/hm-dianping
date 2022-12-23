package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
     private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {

        //缓存穿透
        //Shop shop = queryWithPassThrough(id);
        //互斥锁解决缓存击穿问题
        // Shop shop = queryWithMutex(id);

        //逻辑过期解决缓存击穿问题
        Shop shop = queryWithLogicExpire(id);
        if (shop == null) {
           return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    //线程池
    private static final ExecutorService CACHE_REBULID_EXECUTOR = Executors.newFixedThreadPool(10);


    //逻辑过期解决缓存击穿问题
    private Shop queryWithLogicExpire(Long id){
        //1、根据id查询redis缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        //2、如果不存在，返回空
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }
        //3、如果存在，就要判断逻辑过期时间
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        //获取逻辑过期时间
        LocalDateTime expireTime = redisData.getExpireTime();
        //获取要查询的shop数据
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        //判断如果逻辑过期时间未过期，直接返回查到的数据
        if(expireTime.isAfter(LocalDateTime.now())){
            return shop;
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
                    this.saveShopToRedis(id,20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放锁
                    deleteLock(lockKey);
                }
            });

        }
        //返回
        return shop;
    }

    //互斥锁解决缓存击穿问题
    private Shop queryWithMutex(Long id){
        //1、根据id查询redis缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        //2、如果存在，返回
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //多加一个判断是否为空值的可能，如果为空值返回错误
        if(shopJson != null){
            return null;
        }
        //4、实现缓存重建
        //4、1）尝试获取互斥锁，若不能获取就休眠一会，在重新查询
        String lockKey = "lock:shop:" +id;
        Shop shop = null;
        try {
            boolean lock = tryLock(lockKey);
            if(!lock){
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //模拟重建延时
            Thread.sleep(200);

            //   2）若能获取到互斥锁，再次搜索redis，做二次查验
            //1、根据id查询redis缓存
            shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
            //2、如果存在，返回
            if (StrUtil.isNotBlank(shopJson)) {
                return JSONUtil.toBean(shopJson, Shop.class);
            }
            //多加一个判断是否为空值的可能，如果为空值返回错误
            if(shopJson != null){
                return null;
            }
            //   就根据id查询数据库，查询到的数据写入缓存，之后释放锁
            shop = getById(id);
            // 数据库中不存在
            if (shop==null) {
                //空值写入redis
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,"" ,CACHE_NULL_TTL, TimeUnit.MINUTES);
                //返回空对象
                return null;
            }
            //5、数据库中存在，把数据写入redis，并返回
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //释放锁
            deleteLock(lockKey);
        }
            //返回
            return shop;
    }
    private Shop queryWithPassThrough(Long id){
        //1、根据id查询redis缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        //2、如果存在，返回
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);

        }
        //多加一个判断是否为空值的可能，如果为空值返回错误
        if(shopJson != null){
            return null;
        }
        //3、如果不存在，就去查询数据库
        Shop shop = getById(id);
        //4、数据库中不存在
        if (shop==null) {
            //空值写入redis
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,"" ,CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回空对象
            return null;
        }
        //5、数据库中存在，把数据写入redis，并返回
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
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

    //缓存预热，把数据先存入缓存
    public void saveShopToRedis(Long id,Long expireSeconds) throws InterruptedException {
        //查数据库的信息
        Shop shop = getById(id);
        Thread.sleep(200);
        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        redisData.setData(shop);
        //存入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData));
    }




    @Override
    @Transactional
    public Result update(Shop shop) {

        Long id = shop.getId();
        if(id==null){
            return Result.fail("店铺id不能为空");
        }
        //更新数据库
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
