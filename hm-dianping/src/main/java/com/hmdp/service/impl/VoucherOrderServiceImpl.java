package com.hmdp.service.impl;

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
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
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
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    private final RedisIdWorker redisIdWorker;
    public VoucherOrderServiceImpl(RedisIdWorker redisIdWorker) {
        this.redisIdWorker = redisIdWorker;
    }
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    private static final DefaultRedisScript<Long> SECILL_SCRIPT;
    static {
        SECILL_SCRIPT=new DefaultRedisScript<>();
        SECILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECILL_SCRIPT.setResultType(Long.class);
    }
    private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024*1024);
    private static final ExecutorService SECKILL_ORDER_EXECUTOR= Executors.newSingleThreadExecutor();
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    private class VoucherOrderHandler implements Runnable{
        @Override
        public void run(){
            while (true){
                try {
                    //获取队列中的订单信息
                    VoucherOrder voucherOrder=orderTasks.take();
                    //创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                }
            }
        }

        private void handleVoucherOrder(VoucherOrder voucherOrder) {
            //获取用户
            Long userId=voucherOrder.getUserId();
            //创建锁对象
            //SimpleRedisLock lock=new SimpleRedisLock("order"+userId,stringRedisTemplate);
            RLock lock= redissonClient.getLock("lock:order"+userId);
            //获取锁
            boolean isLock=lock.tryLock();
            //判断是否获取锁成功
            if (!isLock){
                //获取失败，返回错误
                log.error("不允许重复下单");
                return;
            }
            try {
                proxy.createVoucherOrder(voucherOrder);
            }finally {
                lock.unlock();
            }
        }
    }


    private IVoucherOrderService proxy;
    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId=UserHolder.getUser().getId();
        //执行lua脚本
        Long result=stringRedisTemplate.execute(
                SECILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString()
        );
        //判断结果是否为0
        int r=result.intValue();
        if (r!=0){
            //判断结果不为0,没有购买资格
            return Result.fail(r==1?"库存不足":"不能重复下单");
        }
        //为0,有购买资格,把下单信息保存到库存队列
        VoucherOrder voucherOrder=new VoucherOrder();
        //订单id
        long orderId=redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //用户id
        voucherOrder.setUserId(userId);
        //代金卷id
        voucherOrder.setVoucherId(voucherId);
        //放入阻塞队列
        orderTasks.add(voucherOrder);
        //获取代理对象（事务）
        proxy=(IVoucherOrderService) AopContext.currentProxy();
        //返回订单id
        return Result.ok(orderId);
    }

    /*@Override
        public Result seckillVoucher(Long voucherId) {
            //查询优惠卷
            SeckillVoucher voucher=seckillVoucherService.getById(voucherId);
            //判断秒杀是否开始
            if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
                Result.fail("秒杀尚未开始");
            }
            //判断秒杀是否结束
            if (voucher.getEndTime().isBefore(LocalDateTime.now())){
                Result.fail("秒杀已经结束");
            }
            //判断库存是否充足
            if (voucher.getStock()<1){
                Result.fail("秒杀库存不足");
            }

            //返回订单id
            Long userId= UserHolder.getUser().getId();
            //创建锁对象
            //SimpleRedisLock lock=new SimpleRedisLock("order"+userId,stringRedisTemplate);
            RLock lock= redissonClient.getLock("lock:order"+userId);
            //获取锁
            boolean isLock=lock.tryLock();
            //判断是否获取锁成功
            if (!isLock){
                return Result.fail("一个人只允许下一单");
            }
            try {
                //获取（事务）代理对象
                IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
                return proxy.createVoucherOrder(voucherId);
            }finally {
                lock.unlock();
            }
        }*/
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder){
        //一人一单
        Long userId= UserHolder.getUser().getId();
        //查询订单
        int count=query().eq("user_id",userId).eq("voucher_id", voucherOrder).count();
        if (count>0){
            log.error("用户已经购买过一次！");
            return;
        }
        //扣减库存
        boolean success=seckillVoucherService.update()
                .setSql("stock=stock-1")
                .eq("voucher_id", voucherOrder).gt("stock",0)
                .update();
        if (!success){
            log.error("库存不足！");
            return;
        }
        //创建订单
        save(voucherOrder);
        /*VoucherOrder voucherOrder=new VoucherOrder();
        //订单id
        long orderId=redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //用户id
        voucherOrder.setUserId(userId);
        //代金卷id
        voucherOrder.setVoucherId(voucherOrder);*/
        //返回订单id
    }
}
