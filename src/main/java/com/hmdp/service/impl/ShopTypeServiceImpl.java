package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        //1、查询redis缓存中是否有这个shoptype
        Long size = stringRedisTemplate.opsForList().size("cache:shopType");
        List<String> range = stringRedisTemplate.opsForList().range("cache:shopType", 0, size - 1);
        //2、如果有，就返回
        if (!range.isEmpty()) {
            range.stream().map(shopType->{
                return JSONUtil.toBean(shopType,ShopType.class);
            }).collect(Collectors.toList());
            }

        //3、如果没有，就查询数据库
        List<ShopType> list = query().orderByAsc("sort").list();
        //4、数据库中没有，就返回错误
        if (list.isEmpty()) {
            return Result.fail("没有这个店铺类型");
        }
        //5、数据库中有，就写入缓存
        List<String> collect = list.stream().map(item -> {
            return JSONUtil.toJsonStr(item);
        }).collect(Collectors.toList());
        stringRedisTemplate.opsForList().leftPushAll("cache:shopType",collect);
        //6、返回
        return Result.ok(list);
    }
}
