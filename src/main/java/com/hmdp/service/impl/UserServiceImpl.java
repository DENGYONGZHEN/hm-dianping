package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {
    @Resource
     private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //验证手机号是否合法
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号不合法");
        }
        //生成验证码
        String code = RandomUtil.randomNumbers(6);
        //保存到redis中
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY+phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);
        log.debug("生成的验证码；{}",code);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1、校验手机号
        String phone = loginForm.getPhone();
        //验证手机号是否合法
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号不合法");
        }
        //  从redis获取并校验验证码
        String code = loginForm.getCode();
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY+phone);
        if(cacheCode==null || !cacheCode.equals(code)){
            return Result.fail("验证码不正确");
        }
        //3、根据手机号查询数据库，看是否存在
        User user = query().eq("phone", phone).one();
        //如果不存在，就创建并保存
        if (user == null) {
            user = createUserWithPhone(phone);
        }
        // 保存用户信息到redis
        //随机生成token，作为登陆令牌
        String token = UUID.randomUUID().toString(true);
        //将user对象转为hash存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        //在对象转map时，需要设置
        Map<String, Object> map = BeanUtil.beanToMap(userDTO, new HashMap<>(), CopyOptions.create()
                .setIgnoreNullValue(true).setFieldValueEditor((fieldName,fieldValue)->fieldValue.toString()));
        //存储数据到redis,
        stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY+token,map);
        //设置数据有效期
        stringRedisTemplate.expire(LOGIN_USER_KEY+token,30,TimeUnit.MINUTES);
        //返回token
        return Result.ok(token);
    }

    /**
     * 签到功能
     */
    @Override
    public Result signIn() {
        //获取当前用户id
        Long userId = UserHolder.getUser().getId();
        //获取当前年月为key
        LocalDateTime now = LocalDateTime.now();
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY+userId + keySuffix;
        //获取当前日期作为要存在bitMap上的数据的依据
        int dayOfMonth = now.getDayOfMonth();
        //写入redis
         stringRedisTemplate.opsForValue().setBit(key,dayOfMonth-1,true);
        return Result.ok();

    }

    /**
     *
     * @return  查询连续签到次数
     */

    @Override
    public Result signCount() {
        //获取当前用户id
        Long userId = UserHolder.getUser().getId();
        //获取当前年月为key
        LocalDateTime now = LocalDateTime.now();
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY+userId + keySuffix;
        //获取当前日期
        int dayOfMonth = now.getDayOfMonth();
        //获取本月至今为止的所有的签到记录，返回的是一个十进制的数
        List<Long> list = stringRedisTemplate.opsForValue().bitField(key,
                BitFieldSubCommands.create().get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth))
                        .valueAt(0)
        );
        if (list == null || list.isEmpty()) {
            return Result.ok(0);
        }
        Long num = list.get(0);
        if (num == null || num ==0) {
            return Result.ok(0);
        }
        int count  = 0;
        //循环遍历
        //让这个数与1做运算，得到的数字的最后一个bit位，然后这个数向右移一位，再与1做与运算，直到运算结果为0，这样算出连续签到次数
        while (true){
           if((num & 1)==0) {
               break;
           }
            count++;
            num >>>= 1;
        }
        //返回
        return Result.ok(count);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        //这里用的时myBatis-plus的方法
        save(user);
        return user;
    }
}
