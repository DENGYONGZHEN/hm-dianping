package com.hmdp.config;



import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {
    @Bean
    public RedissonClient redissonClient(){
        //先配置  配置信息
        Config config = new Config();
        config.useSingleServer().setAddress("redis://192.168.206.130:65534").setPassword("root");
        //创建RedissonClient
        return Redisson.create(config);
    }
}
