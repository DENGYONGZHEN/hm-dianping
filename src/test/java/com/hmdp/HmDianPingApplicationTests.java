package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;


@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
   private ShopServiceImpl shopService;
    @Resource
    private CacheClient cacheClient;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private ExecutorService es = Executors.newFixedThreadPool(500);


    @Test
    public void testUniqueId() throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(300);
        Runnable task = ()->{
            for (int i = 0; i < 100; i++) {
                long order = redisIdWorker.nextId("order");
                System.out.println(order);
            }
            countDownLatch.countDown();
        };
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        countDownLatch.await();
        long end = System.currentTimeMillis();
        System.out.println("time= "  + (end-begin));


    }

    @Test
    public void test2() throws InterruptedException {
        Shop shop = shopService.getById(1L);
        cacheClient.setKeyLogicExpire(RedisConstants.CACHE_SHOP_KEY +1L,shop,10L, TimeUnit.SECONDS);

    }

    /**
     * 店铺数据根据店铺类型导入redis的geo数据结构中
     */
    @Test
    public void dataToRedis(){
        //1、查询所有数据
        List<Shop> shopList = shopService.list();
        //2、根据typeId分组
        Map<Long,List<Shop>> map = shopList.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        //3、分批写入redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            //获取店铺类型typeId
            Long typeId = entry.getKey();
            String key = SHOP_GEO_KEY+typeId;
            //获取类型相同的店铺对象集合
            List<Shop> shops = entry.getValue();

            List<RedisGeoCommands.GeoLocation<String>> geolocations = new ArrayList(shops.size());
            //分批写入redis
            for (Shop shop : shops) {
                //stringRedisTemplate.opsForGeo().add(key,new Point(shop.getX(),shop.getY()),shop.getId().toString());
                geolocations.add(new RedisGeoCommands.GeoLocation<>(shop.getId().toString(),
                                                                                                           new Point(shop.getX(),shop.getY())));
            }
            stringRedisTemplate.opsForGeo().add(key,geolocations);
        }
    }
}
