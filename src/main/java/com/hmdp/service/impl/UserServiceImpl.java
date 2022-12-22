package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;

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

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //验证手机号是否合法
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号不合法");
        }
        //生成验证码
        String code = RandomUtil.randomNumbers(6);
        session.setAttribute("code",code);
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
        //2、校验验证码
        String code = loginForm.getCode();
        Object cacheCode = session.getAttribute("code");
        if(cacheCode==null || !cacheCode.toString().equals(code)){
            return Result.fail("验证码不正确");
        }
        //3、根据手机号查询数据库，看是否存在
        User user = query().eq("phone", phone).one();
        //如果不存在，就创建并保存
        if (user == null) {
            user = createUserWithPhone(phone);
        }
        session.setAttribute("user", BeanUtil.copyProperties(user,UserDTO.class));
        return Result.ok();
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
