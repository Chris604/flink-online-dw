package task;

import bean.*;
import kafka.KafkaConsumer;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import sink.KuduSink;

import java.util.HashMap;
import java.util.Map;

// 两条数据流 join
public class StreamJoin {

    public static void main(String[] args) throws Exception {
        // 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 接入数据源
        SingleOutputStreamOperator<UserAction> leftMap = env.addSource(KafkaConsumer.consumer(CONSTANT.BROKERS, CONSTANT.GROUPID, "newsearn_decode"))
                .map(new MyMap())
                .filter(item -> (item.userId != null && "AppClick".equals(item.action) && item.articleId != null));

        SingleOutputStreamOperator<Experiment> rightMap = env.addSource(KafkaConsumer.consumer(CONSTANT.BROKERS, CONSTANT.GROUPID, "abtest"))
                .map(new MyMap.ExpMap())
                .filter(exp -> "内容推荐".equals(exp.domainName));

        // join
        DataStream<JoinResult> output = leftMap.join(rightMap)
                .where(user -> user.userId)
                .equalTo(exp -> exp.userId)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(10000)))
                .apply(new JoinFunction<UserAction, Experiment, JoinResult>() {
                    JoinResult joinResult = new JoinResult();

                    @Override
                    public JoinResult join(UserAction first, Experiment second) throws Exception {
                        joinResult.userId = first.userId;
                        joinResult.articleId = first.articleId;
                        joinResult.action = first.action;
                        joinResult.expName = second.experimentName;
                        return joinResult;
                    }
                });

//        output.print();

        //

        // sink 到 kudu
        String kuduMaster = CONSTANT.KUDUMASTER;
        String tableName = CONSTANT.TABLENAME;

        output.map(new MapFunction<JoinResult, Map<String, Object>>() {
            Map<String, Object> map = new HashMap<>();
            @Override
            public Map<String, Object> map(JoinResult value) throws Exception {
                map.put("userid", value.userId);
                map.put("article_id", value.articleId);
                map.put("action", value.action);
                map.put("exp_name", value.expName);
                return map;
            }
        }).addSink(new KuduSink(kuduMaster, tableName));

        // Send hdfs by parquet
//        String path = "hdfs://idc-001-namenode1:8020/user/hive/warehouse/recommend.db/parquet_f_flink";
//        String pathFormat = "yyyy-MM-dd";
//        String zone = "Asia/Shanghai";
//
//        DateTimeBucketAssigner<JoinResult> bucketAssigner = new DateTimeBucketAssigner<>(pathFormat, ZoneId.of(zone));
//        StreamingFileSink<JoinResult> streamingFileSink = StreamingFileSink.forBulkFormat(new Path(path), ParquetAvroWriters.forReflectRecord(JoinResult.class)).withBucketAssigner(bucketAssigner).build();
//        output.addSink(streamingFileSink).name("Sink To HDFS");

        env.execute("join-test");
    }
}