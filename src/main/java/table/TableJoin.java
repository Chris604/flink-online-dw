package table;

import bean.*;
import kafka.KafkaConsumer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class TableJoin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        // 引入两条数据源
        SingleOutputStreamOperator<UserAction> leftMap = env.addSource(KafkaConsumer.consumer(CONSTANT.BROKERS, CONSTANT.GROUPID, "newsearn_decode"))
                .map(new MyMap())
                .filter(item -> (item.userId != null && "AppClick".equals(item.action) && item.articleId != null));

        SingleOutputStreamOperator<Experiment> rightMap = env.addSource(KafkaConsumer.consumer(CONSTANT.BROKERS, CONSTANT.GROUPID, "abtest"))
                .map(new MyMap.ExpMap())
                .filter(exp -> "内容推荐".equals(exp.domainName));

        // 注册 table
        tEnv.registerDataStream("useraction", leftMap);
        tEnv.registerDataStream("experiment", rightMap);

        // 两表关联
        String sql = "select experiment.userId, useraction.articleId, useraction.action, experiment.experimentName as expName from experiment join useraction on experiment.userId = useraction.userId";

//        String sql = "select userId, count(1) as  `count` from useraction group by userId";
        Table table = tEnv.sqlQuery(sql);

        DataStream<Tuple2<Boolean, JoinResult>> tuple2DataStream = tEnv.toRetractStream(table, JoinResult.class);
//        DataStream<Tuple2<Boolean, ActionStat>> tuple2DataStream = tEnv.toRetractStream(table, ActionStat.class);

        tuple2DataStream.print();

        env.execute("sql-test");
    }
}
