package com.lin.test;

import com.lin.entity.User;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author lzr
 * @date 2020-09-09 23:58:21
 */
public class MyRedisSinkMapper implements RedisMapper<User> {

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "flink-test");
    }

    @Override
    public String getKeyFromData(User user) {
        return user.getId().toString();
    }

    @Override
    public String getValueFromData(User user) {
        return user.getUsername();
    }

}
