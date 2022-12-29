package com.hmdp.service.impl;


import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private  StringRedisTemplate stringRedisTemplate;
    @Resource
     private RedissonClient redissonClient;



    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    //静态加载初始化脚本对象
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    //创建阻塞队列
   // private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);
  /*  private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while (true){
                try {
                    //获取对列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    //创建订单
                    handleVoucherOrder(voucherOrder);

                } catch (Exception e) {
                    log.error("处理订单异常",e) ;
                }


            }
        }
    }

   */


    //创建线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR  = Executors.newSingleThreadExecutor();
    //创建线程任务
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    private class VoucherOrderHandler implements Runnable{
        String queueName = "stream.order";
        @Override
        public void run() {
            while (true){
                try {
                    //1、获取消息队列中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2、判断消息是否获取成功
                    if(list ==null || list.isEmpty()){
                        continue;
                    }
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //3、创建订单
                    handleVoucherOrder(voucherOrder);
                    //4、ack确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());

                } catch (Exception e) {
                    log.error("读取消息队列和创建订单异常",e) ;
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true){
                try{
                    //1、获取pedingList中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2、如果没有读取到消息，说明没有异常消息，结束循环
                    if(list ==null || list.isEmpty()){
                        break;
                    }
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //3、创建订单
                    handleVoucherOrder(voucherOrder);
                    //4、ack确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                }catch (Exception e){
                    log.error("处理pendingList异常",e) ;
                }
            }
        }
    }



    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //获取用户
        //子线程不能从ThreadLocal中取数据了
        Long userId =voucherOrder.getUserId() ;
        //使用redisson获取锁
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            //未成功获取锁，返回错误信息
            log.error("不允许重复下单");
            return ;
        }
        try {
            //事务代理对象先在父线程中获取
             proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 用lua脚本保证操作原子性来操作判断用户是否有购买资格
     * @param voucherId
     * @return
     */

    //spring事务代理对象
    private IVoucherOrderService  proxy;
    @Override
    public Result secKillVoucher(Long voucherId) {

        //获取用户
        Long userId = UserHolder.getUser().getId();
        //设置id，通过全局唯一的id生成器生成的id
        long orderId = redisIdWorker.nextId("order");
        //执行lua脚本
        Long execute = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(),
                 voucherId.toString(), userId.toString(),String.valueOf(orderId));    //基本类型不能直接使用toString方法
        int value = execute.intValue();
        //判断执行lua脚本得到的结果  0，是可以下单，1是库存不足，2是已经购买，不能重复下单
        if(value !=0){
            return Result.fail(value == 1 ?"库存不足":"不能重复下单");
        }
        //spring的事务是生效在代理接口上，实现类没有事务功能，所以要用代理对象来调用
        proxy= (IVoucherOrderService) AopContext.currentProxy();
        //返回订单id
        return Result.ok(orderId);
    }


    /*
    @Override
    public Result secKillVoucher(Long voucherId) {

        //获取用户
        Long userId = UserHolder.getUser().getId();

        //执行lua脚本
        Long execute = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId.toString());
        int value = execute.intValue();
        //判断执行lua脚本得到的结果  0，是可以下单，1是库存不足，2是已经购买，不能重复下单
        if(value !=0){
           return Result.fail(value == 1 ?"库存不足":"不能重复下单");
        }

        //  结果为0，下单信息保存到阻塞队列中
        //新建订单
        //设置id，通过全局唯一的id生成器生成的id
        long orderId = redisIdWorker.nextId("order");
        VoucherOrder voucherOrder = new VoucherOrder();
        //订单id
        voucherOrder.setId(orderId);
        //代金券id
        voucherOrder.setVoucherId(voucherId);
        //用户id
        voucherOrder.setUserId(userId);
        //放入阻塞队列
        orderTasks.add(voucherOrder);
        //spring的事务是生效在代理接口上，实现类没有事务功能，所以要用代理对象来调用
         proxy= (IVoucherOrderService) AopContext.currentProxy();
        //返回订单id
        return Result.ok(orderId);
    }  */

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //子线程不能从ThreadLocal中取数据了
        Long userId = voucherOrder.getUserId();
        //保证一人一单的功能
        //通过用户id和voucher 的id查询数据库订单表
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            log.error("用户已经购买过一次");
            return ;
        }
        //更新库存
        boolean success = seckillVoucherService.update().setSql("stock=stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();
        if (!success) {
            //没库存返回错误
            log.error("库存不足");
            return ;
        }
        //订单写入数据库
        save(voucherOrder);
    }


  /*    java代码判断是否有购买资格
    @Override
    public Result secKillVoucher(Long voucherId) {

        //根据id查询秒杀优惠券信息
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        //获取秒杀优惠券的开始时间
        LocalDateTime beginTime = seckillVoucher.getBeginTime();
        //判断秒杀的开始时间，没开始就返回错误
        if (LocalDateTime.now().isBefore(beginTime)) {
            return Result.fail("活动还未开始");
        }
        //判断秒杀的结束时间，如果结束，返回错误
        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("活动已结束");
        }
        //获取秒杀券的库存
        Integer stock = seckillVoucher.getStock();
        //判断库存
        if(stock< 1){
            //没库存返回错误
            return Result.fail("券已售完");
        }

        Long userId = UserHolder.getUser().getId();
        //加上悲观锁，确保串行执行，userId.toString().intern()用id的值作为锁 ,然后创建事务
        //synchronized (userId.toString().intern()) {


        //创建redis锁，通过redis锁来实现分布式的线程安全
       // SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        //使用redisson获取锁
         RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            //未成功获取锁，返回错误信息
            return Result.fail("不允许重复下单");
        }
        try {
            //spring的事务是生效在代理接口上，实现类没有事务功能，所以要用代理对象来调用
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }
    } */

/*
    @Transactional
    public  Result createVoucherOrder(Long voucherId) {

        Long userId = UserHolder.getUser().getId();
            //保证一人一单的功能
            //通过用户id和voucher 的id查询数据库订单表
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            if (count > 0) {
                return Result.fail("用户已经买过一次了");
            }
            //更新库存
            boolean success = seckillVoucherService.update().setSql("stock=stock - 1")
                    .eq("voucher_id", voucherId).gt("stock", 0)
                    .update();
            if (!success) {
                //没库存返回错误
                return Result.fail("券已售完");
            }
            //新建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            //设置id，通过全局唯一的id生成器生成的id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            voucherOrder.setVoucherId(voucherId);
            //设置user_id,通过用户拦截器UserHolder获取用户id
            voucherOrder.setUserId(userId);
            //订单写入数据库
            save(voucherOrder);
            //返回订单
            return Result.ok(orderId);

    } */
}
