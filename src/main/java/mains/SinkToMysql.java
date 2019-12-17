package mains;

import bean.ActionStat;
import bean.UserAction;
import com.alibaba.fastjson.JSONObject;
import kafka.KafkaConsumer;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import sink.MysqlSink;

import javax.annotation.Nullable;


public class SinkToMysql {

    public static void main(String[] args) throws Exception {
        String brokers;
        String groupId;
        String topic;
        if (args.length > 0){
            ParameterTool parameters = ParameterTool.fromArgs(args);
            brokers = parameters.get("brokers");
            groupId = parameters.get("groupId");
            topic = parameters.get("topic");
        } else {
            brokers = "192.168.10.7:9092,192.168.10.8:9092,192.168.10.9:9092";
            groupId = "online-test";
            topic = "newsearn_decode";
        }

        // 创建 flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 连接数据源
        DataStream dataStream = env.addSource(KafkaConsumer.consumer(brokers, groupId, topic));

        SingleOutputStreamOperator map = dataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
            Long currTime = 0L;
            Long offset = 1000L;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currTime - offset);
            }

            @Override
            public long extractTimestamp(String element, long previousElementTimestamp) {
                JSONObject jsonObject = JSONObject.parseObject(element);
                if ("newsearn".equals(jsonObject.get("project"))) {
                    if (!"".equals(jsonObject.get("content"))) {
                        JSONObject content = JSONObject.parseObject(jsonObject.get("content").toString());
                        if (content.getString("time") != null) {
                            currTime = Long.parseLong(content.getString("time"));
                        }
                    }
                }
                return currTime;
            }
        }).filter(s -> (s.toString().contains("newsearn") && s.toString().contains("AppClick")))
                .map(new MyMap());

        SingleOutputStreamOperator aggregate = map.filter(new FilterFunction<UserAction>() {

            @Override
            public boolean filter(UserAction value) throws Exception {
                return value.userId != null;
            }
        }).keyBy("userId")
          .timeWindow(Time.milliseconds(1000))
          .aggregate(new MyAggr());

        aggregate.print();
        aggregate.addSink(new MysqlSink());

        env.execute("flink-online");
    }

    // 自定义 map 函数
    static class MyMap implements MapFunction<String, UserAction> {

        @Override
        public UserAction map(String value) throws Exception {

            JSONObject jsonObject = JSONObject.parseObject(value);
            JSONObject content = JSONObject.parseObject(jsonObject.getString("content"));
            if (jsonObject.getString("content") != null){
                JSONObject properties = JSONObject.parseObject(content.getString("properties"));
                String userId = properties.getString("userId");
                String articleId = properties.getString("article_id");
                String action = content.getString("event");

                UserAction us = new UserAction(userId, articleId, action);
                return us;
            }
            return null;
        }
    }

    static class MyAggr implements AggregateFunction<UserAction, ActionStat, ActionStat>{

        ActionStat actionStat = new ActionStat();
        Long cnt = 0L;

        @Override
        public ActionStat createAccumulator() {
            return actionStat;
        }

        @Override
        public ActionStat add(UserAction value, ActionStat accumulator) {
            accumulator.userId = value.userId;
            cnt = cnt + 1;
            accumulator.count = cnt;
            return accumulator;
        }

        @Override
        public ActionStat getResult(ActionStat accumulator) {
            return accumulator;
        }

        @Override
        public ActionStat merge(ActionStat a, ActionStat b) {

            actionStat.count = a.count + b.count;
            return actionStat;
        }
    }
}