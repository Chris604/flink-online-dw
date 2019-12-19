package task;

import bean.ActionStat;
import bean.CONSTANT;
import bean.MyMap;
import bean.UserAction;
import kafka.KafkaConsumer;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import util.MyRedisMapper;


// 处理数据并写入到 redis
public class SinkToRedis {

    public static void main(String[] args) throws Exception {
        // 初始化 flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从 kafka 获取数据

        String brokers;
        String groupId;
        String topic;

        ParameterTool param = ParameterTool.fromArgs(args);
        if (param.equals(null)){
            brokers = param.get("brokers");
            groupId = param.get("groupId");
            topic   = param.get("topic");
        } else {
            brokers = "";
            groupId = "";
            topic = "";
        }

        // 消费 kafka，接入数据源
        DataStream<String> dataStream = env.addSource(KafkaConsumer.consumer(brokers, groupId, topic));

        SingleOutputStreamOperator<ActionStat> userStat = dataStream.map(new MyMap())
                .filter(user -> (user.userId != null && user.articleId != null && "AppClick".equals(user.action)))
                .keyBy("userId")
                .timeWindow(Time.milliseconds(5000))
                .aggregate(new AggDiY());

        userStat.print();

        // 初始化 redis 配置
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                    .setHost(CONSTANT.REDIS_HOST)
                    .setPort(CONSTANT.REDIS_PORT)
                    .setPassword(CONSTANT.PASSWORD)
                    .setDatabase(0)
                    .build();

        userStat.addSink(new RedisSink<>(jedisPoolConfig, new MyRedisMapper(RedisCommand.SET)));

        env.execute("filnk-test");
    }

    // 按 userid 统计
    static class AggDiY implements AggregateFunction<UserAction, ActionStat, ActionStat>{

        Long count = 0L;

        @Override
        public ActionStat createAccumulator() {
            return new ActionStat();
        }

        @Override
        public ActionStat add(UserAction value, ActionStat accumulator) {
            accumulator.userId = value.userId;
            accumulator.count = ++count;
            return accumulator;
        }

        @Override
        public ActionStat getResult(ActionStat accumulator) {
            return accumulator;
        }

        @Override
        public ActionStat merge(ActionStat a, ActionStat b) {
            a.count = a.count + b.count;
            return a;
        }
    }
}
