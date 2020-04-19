package myflink.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 基本转换算子API
 *
 * @Author jiahao
 * @Date 2020/4/19 11:39
 */
public class BaseTransformApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 指定数据源
        DataStreamSource<String> dataStreamSource = env.readTextFile("D:\\flink\\flinkLearnProject\\src\\main\\resources\\testfile");
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = dataStreamSource.map(each -> {
            String[] split = each.split(",");
            return Tuple2.of(split[0], Integer.valueOf(split[1]));
        }).returns(Types.TUPLE(Types.STRING,Types.INT));

        // 按照name进行分组
        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = map.keyBy(0);
        tuple2TupleKeyedStream.sum(1).print();

        env.execute("BaseTransformApi_1");

    }
    static class NameNumSource{
        private String name;
        private int num;

        public NameNumSource(String name, Integer num) {
            this.name = name;
            this.num = num;
        }

        @Override
        public String toString() {
            return "NameNumSource{" +
                    "name='" + name + '\'' +
                    ", num=" + num +
                    '}';
        }
    }
}
