package myflink.transform;

import org.apache.flink.api.common.functions.ReduceFunction;
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
 * 聚合算子reduce
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
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = map.keyBy(0);
//        .sum(1);
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                // 上一个name与下一个name拼接起来，并且num相加
                return Tuple2.of(value1.f0 + "_" + value2.f0, value2.f1 + value1.f1);
            }
        });
        reduce.print();

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
