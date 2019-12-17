package kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;

import java.util.Properties;

// kafka 消费者
public class KafkaConsumer {

    public static FlinkKafkaConsumer09<String> consumer(String brokers, String groupId, String topic){
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", brokers);
//        prop.setProperty("zookeeper.connect", zk);
        prop.setProperty("group.id", groupId);

        FlinkKafkaConsumer09<String> consumer = new FlinkKafkaConsumer09(topic, new SimpleStringSchema(), prop);
        consumer.setStartFromLatest();
        return consumer;
    }
}
