package myflink.udfFunction;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 富函数的使用
 *
 * @Author jiahao
 * @Date 2020/5/9 20:46
 */
public class RichFunctionsTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.readTextFile("D:\\flink\\flinkLearnProject\\src\\main\\resources\\testfile");

        SingleOutputStreamOperator<Integer> returns = dataStreamSource.map(new MapRichFunction()).returns(Integer.class);
        returns.print();
        env.execute();
    }
    static class MapRichFunction extends RichMapFunction<String,Integer> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("do open");
        }

        @Override
        public void close() throws Exception {
            System.out.println("do close");
        }

        @Override
        public Integer map(String s) throws Exception {
            return 1;
        }
    }
}

