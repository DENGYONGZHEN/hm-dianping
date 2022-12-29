package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
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
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {


    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IUserService userService;

    /**
     *
     * @param followUserId  被关注的用户id
     * @param isFollow   是否关注，前端传过来的值
     * @return
     */
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        //获取登录用户
        Long userId = UserHolder.getUser().getId();
        String key = "blog:follows:" +userId;
        //判断到底是关注还是取关
        if (BooleanUtil.isTrue(isFollow)) {
            //关注，增加数据
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean isSuccess = save(follow);
            if (isSuccess) {
                //把被当前登录用户 关注的用户的id，放入redis的set中
                stringRedisTemplate.opsForSet().add(key,followUserId.toString());
            }
        }else {
            //取关，删除
            boolean isSuccess = remove(new QueryWrapper<Follow>().eq("user_id", userId).eq("follow_user_id", followUserId));
            //把被关注的用户id从redis中移除
            if (isSuccess) {
                stringRedisTemplate.opsForSet().remove(key,followUserId.toString());
            }
        }
        return Result.ok();
    }


    /**
     * 查询是否关注了
     * @param followUserId   关注的用户id
     * @return
     */
    @Override
    public Result isFollow(Long followUserId) {
        //获取登录用户
        Long userId = UserHolder.getUser().getId();
        //查询是否关注
        Integer count = query().eq("user_id", userId).eq("follow_user_id", followUserId).count();

        return Result.ok(count > 0);
    }

    /**
     * 查看当前登录用户和目标用户的共同好友
     * @param id  目标用户id
     * @return
     */
    @Override
    public Result commonFollows(Long id) {
        //获取当前用户
        Long userId = UserHolder.getUser().getId();
        //当前用户在redis中关注用户的集合的key值
        String key = "blog:follows:" +userId;
        //利用redis的set的求交集方法，查出共同关注
        Set<String> commonFollow = stringRedisTemplate.opsForSet().intersect(key, "blog:follows:" + id);
        if (commonFollow == null ||commonFollow.isEmpty()) {
            //无交集，return空
            return Result.ok(Collections.emptyList());
        }
        //转换成id的原来类型Long
        List<Long> comonIds = commonFollow.stream().map(Long::valueOf).collect(Collectors.toList());
        //查出用户，并转换为可以返回的对象类型
        List<UserDTO> userDTOS = userService.listByIds(comonIds).stream().map(user ->
                BeanUtil.copyProperties(user, UserDTO.class)
        ).collect(Collectors.toList());
//        返回查询结果
        return Result.ok(userDTOS);
    }
}
