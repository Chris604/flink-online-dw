package sql;

import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

// sql
public class SinkWithSql {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = TableEnvironment.getTableEnvironment(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<Tuple3<String, String, Integer>> dataStream = env.fromElements(new Tuple3<>("Jack", "男", 23),
                new Tuple3<>("Cris", "男", 22),
                new Tuple3<>("Tom", "男", 24),
                new Tuple3<>("Lucy", "女", 21));

        Table table = tenv.fromDataStream(dataStream, "name,sex,age");
        tenv.registerTable("student", table);
        tenv.registerTable("stu", table);

        String sql = "select * from student";

        Table table1 = tenv.sqlQuery(sql);

        table1.where("name='Cris'");

        table1.insertInto("stu");

//        tuple2DataStream.print();

//        env.execute("test");
    }
}
