package com.hmdp.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public Result queryAll() {
        String key=CACHE_SHOP_KEY +"type";
        String jsonStr=stringRedisTemplate.opsForValue().get(key);
        List<ShopType> typeList;
        if (jsonStr!=null){
            try{
                //String转list
                typeList=objectMapper.readValue(jsonStr, new TypeReference<List<ShopType>>() {
                });
            } catch (JsonProcessingException e) {
                throw new RuntimeException("缓存解析失败",e);
            }
        }else{
            //没有缓存查数据库
            typeList=this.query().orderByAsc("sort").list();
            try{
                String json=objectMapper.writeValueAsString(typeList);
                stringRedisTemplate.opsForValue().set(key,json);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("缓存写入失败",e);
            }
        }
        return Result.ok(typeList);
    }
}
