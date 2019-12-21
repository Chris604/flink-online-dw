package task;

import bean.CONSTANT;
import bean.MyMap;
import bean.UserAction;
import com.alibaba.fastjson.JSONObject;
import kafka.KafkaConsumer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;

public class SinkToHbase {
    public static void main(String[] args) throws Exception {

        // 初始化 flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000);
        env.setParallelism(1);

        // 从 kafka 获取数据
        ParameterTool param = ParameterTool.fromArgs(args);
        String brokers = param.get("brokers");
        String groupId = param.get("groupId");
        String topic   = param.get("topic");

        SingleOutputStreamOperator<String> dataStream = env.addSource(KafkaConsumer.consumer(brokers, groupId, topic))
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
                    Long currTime= 0L ;
                    Long offSet = 0L;

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

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currTime - offSet);
                    }
                });

        SingleOutputStreamOperator<UserAction> appClick = dataStream.filter(action -> action.contains("AppClick"))
                .map(new MyMap());

        appClick.print();

        appClick.addSink(new HBaseOutputFormat());

        env.execute("flink-test");
    }

    /**
     * This class implements an OutputFormat for HBase.
     */
    public static class HBaseOutputFormat extends RichSinkFunction<UserAction> {

        private static final Logger logger = LoggerFactory.getLogger(HBaseOutputFormat.class);

        private static Configuration conf = null;
        private static Connection conn = null;
        private static Table table = null;

        static {
            conf = HBaseConfiguration.create();
//            conf.set("hbase.rootDir", "hdfs://bj-dcs-001:8020/hbase");
            conf.set(HConstants.ZOOKEEPER_QUORUM, CONSTANT.ZOOKEEPER_QUORUM);
            conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, CONSTANT.ZOOKEEPER_CLIENT_PORT);
            conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, CONSTANT.ZOOKEEPER_ZNODE_PARENT);
            try {
                conn = ConnectionFactory.createConnection(conf);
                table = conn.getTable(TableName.valueOf("user_action"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void invoke(UserAction record) throws IOException {
            Put put = new Put(Bytes.toBytes(record.userId));
            put.addColumn(Bytes.toBytes("app"), Bytes.toBytes("action"), Bytes.toBytes(record.toString()));
            table.put(put);
        }

        @Override
        public void close() throws IOException {
            if (null != table) table.close();
            if (null != conn) conn.close();
            logger.info("关闭 hbase 连接！");
        }
    }
}
