package util;

import bean.ActionStat;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class MyRedisMapper implements RedisMapper<ActionStat> {

    private RedisCommand redisCommand;

    public MyRedisMapper(RedisCommand redisCommand){
         this.redisCommand = redisCommand;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(redisCommand, "");
    }

    @Override
    public String getKeyFromData(ActionStat data) {
        return data.userId;
    }

    @Override
    public String getValueFromData(ActionStat data) {
        return data.count.toString();
    }
}
