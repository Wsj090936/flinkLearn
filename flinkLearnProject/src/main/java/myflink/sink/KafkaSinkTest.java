package myflink.sink;

import lombok.Data;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * kafka sink
 *
 * @Author jiahao
 * @Date 2020/5/10 13:55
 */
public class KafkaSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        // kafka sensor1 产生数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer011<String>("sensor1", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<String>  outputStream = dataStreamSource.map(each -> {
            String[] split = each.split(",");
            Person person = new Person();
            person.setName(split[0]);
            person.setAge(Integer.valueOf(split[1]));
            return person.toString();
        }).returns(String.class);
        // kafka sinkTest 输出处理后的数据
        outputStream.addSink(new FlinkKafkaProducer011<String>("localhost:9092","sinkTest",new SimpleStringSchema()));
        outputStream.print();
        env.execute("kafka sink");
    }
    @Data
    static class Person{
        private String name;
        private Integer age;
    }
}
