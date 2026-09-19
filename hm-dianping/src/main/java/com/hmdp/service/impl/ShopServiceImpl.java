package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

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
    @Autowired
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop=cacheClient
        //        .queryWithPassThrough(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);
        //Shop shop=queryWithPassThrough(id);
        //互斥锁解决缓存击穿
        //Shop shop=queryWithMutex(id);
        //逻辑过期解决缓存击穿
        Shop shop=cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY,id,Shop.class,this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);
        if (shop==null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }
    //缓存击穿
    public Shop queryWithMutex(Long id){
        String key=CACHE_SHOP_KEY + id;
        //从redis查询商铺id
        String shopJson=stringRedisTemplate.opsForValue().get(key);
        //判断是否存在,只有有数据时才返回ture,为null或""均为false
        if (StrUtil.isNotBlank(shopJson)){
            Shop shop= JSONUtil.toBean(shopJson,Shop.class);
            return shop;
        }
        //判断命中的是否是空值
        if (shopJson!=null){
            return null;
        }
        //实现缓存重建
        //获取互斥锁
        String lockKey="lock:shop:"+ id;
        Shop shop=null;
        try {
            boolean isLock=tryLock(lockKey);
            //判断获取是否获取成功
            //失败，则休眠重试
            if (!isLock){
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //不存在，根据id查询数据库
            shop=getById(id);
            //模拟重建延时
            Thread.sleep(200);
            if (shop==null){
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }
            //存在写入redis
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        }catch (InterruptedException e){
            throw new RuntimeException(e);
        }finally {
            //释放互斥锁
            unlock(lockKey);
        }
        return shop;
    }
    public Shop queryWithPassThrough(Long id){
        String key=CACHE_SHOP_KEY + id;
        //从redis查询商铺id
        String shopJson=stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if (StrUtil.isNotBlank(shopJson)){
            Shop shop= JSONUtil.toBean(shopJson,Shop.class);
            return shop;
        }
        //判断命中的是否是空值
        if (shopJson!=null){
            return null;
        }
        //不存在，根据id查询数据库
        Shop shop=getById(id);
        if (shop==null){
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //存在写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    private boolean tryLock(String key){
        Boolean flag=stringRedisTemplate.opsForValue().setIfAbsent(key,"1",10,TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id=shop.getId();
        if (id==null){
            return Result.fail("店铺不能为空");
        }
        //更新数据库
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY+id);
        return Result.ok();
    }
}
