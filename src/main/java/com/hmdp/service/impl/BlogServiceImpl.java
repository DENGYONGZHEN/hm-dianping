package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.conditions.update.UpdateChainWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

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
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IFollowService followService;



    @Override
    public Result queryHotBlog(Integer current) {

        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog->{
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在");
        }
        //查询blog有关的用户
        queryBlogUser(blog);
        //查询blog是否被点赞了
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        //获取当前用户
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            //用户未登录，无须查询是否点赞
            return;
        }
        Long userId = user.getId();
        //判断当前用户是否点赞了
        String key   = BLOG_LIKED_KEY +blog.getId() ;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null );
    }

    @Override
    public Result likeBlog(Long id) {
        //获取当前用户
        Long userId = UserHolder.getUser().getId();
        //判断当前用户是否点赞了
        String key   = BLOG_LIKED_KEY +id ;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score == null) {
            //操作数据库,更新点赞人数，增加1
            boolean isSuccess = update().setSql("liked = liked +1").eq("id", id).update();
            if (isSuccess) {
                //保存用户id到redis 的zset中
                stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());
            }
        }else {
            //操作数据库，更新点赞人数，减去1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            if (isSuccess) {
                //从redis中删除用户id
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        //查询前五个的点赞用户
        String key   = BLOG_LIKED_KEY +id ;
        Set<String> range = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if(range == null || range.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        //解析出点赞的用户id
        List<Long> ids = range.stream().map(Long::valueOf).collect(Collectors.toList());
        //根据用户id列表查询出用户列表
        String idStr = StrUtil.join(",",ids);
        //这里查询数据库用的是in，查出来的结果是按id排序的，而想要实现的是按照时间排序，所以增加order by
        List<User> users = userService.query().in("id",ids).last("ORDER BY FIELD(id,"+ idStr+")").list();
        List<UserDTO> userDTOList = users.stream().map(user ->
                BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
        return Result.ok(userDTOList);
    }

    /**
     * 保存探店笔记成功后，把笔记推送给关注当前up主的人
     * @param blog
     * @return
     */
    @Override
    public Result saveBlog(Blog blog) {
        // 1、获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        //2、保存探店博文
        boolean isSuccess = save(blog);
        if (!isSuccess) {
            return Result.fail("新增笔记失败");
        }
        //3、查询当前up主的所有粉丝      "follow_user_id"对应的是被关注的人     user.getId()对应的是当前up主的id
        List<Follow> follows= followService.query().eq("follow_user_id", user.getId()).list();
        //4、推送笔记给粉丝
        for (Follow follow : follows) {
            //4.1、获取粉丝id
            Long userId = follow.getUserId();
            //4.2、推送
            String key = FEED_KEY+userId;
            stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());
        }
        //3、返回id
        return Result.ok(blog.getId());

    }

    /**
     * 查询关注的up推送的笔记
     * @param max
     * @param offset
     * @return
     */
    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 1、获取当前用户id
        Long userId = UserHolder.getUser().getId();
        //2、根据用户id查询存在set中的blogId和score列表
        String key = FEED_KEY+userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet().
                                                                                      reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        //3、解析数据：blogId，mintime,offset
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        //list 创建时指定相同的大小，效率高
        List<Long> blogIds = new ArrayList<>(typedTuples.size());
        //这里初始化为0，下面每次遍历都会覆盖一次，遍历到最后，值就是最小的
        long mintime = 0;
        int os = 1;    //offset 最小有一个
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            //获取blogid并放入集合中
            blogIds.add(Long.valueOf(tuple.getValue()));
            //获取此次循环的score
            long time = tuple.getScore().longValue();
            //判断和上次循环的score的值是否相等
            if(mintime== time){
                os++;
            }else {
                //获取score(时间戳)
                mintime = time;
                os = 1;
            }
        }
        //4、根据blogId查询blog
        String idStr = StrUtil.join(",", blogIds);
        List<Blog> blogs = query().in("id",blogIds).last("ORDER BY FIELD(id,"+ idStr+")").list();
        for (Blog blog : blogs) {
            //查询blog有关的用户
            queryBlogUser(blog);
            //查询blog是否被点赞了
            isBlogLiked(blog);
        }
        //5、封装并返回
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setMinTime(mintime);
        scrollResult.setOffset(os);
        return Result.ok(scrollResult);
    }
    //ctrl +alt +m 直接封装成方法
    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
