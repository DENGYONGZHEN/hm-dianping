package com.hmdp.service.impl;


import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
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
    }

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

    }
}
