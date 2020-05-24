package myflink.processFunction;

import myflink.windowApi.TimeWindowTest;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 *
 * @Author jiahao
 * @Date 2020/5/24 19:39
 */
public class ProcessFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);// 方便测试，设置全局并行度为1
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);// 指定时间语义为eventTime
        DataStreamSource<String> dataStreamSource =
                env.socketTextStream("localhost", 7777, "\n");
        // 从socket中读取数据
        KeyedStream<TimeWindowTest.WindowPerson, String> keyedStream = dataStreamSource.map(new TimeWindowTest.PersonMapFunction())
                .keyBy(new TimeWindowTest.MyKeySelector());
        dataStreamSource.print();

        SingleOutputStreamOperator<String> process = keyedStream.process(new MyKeyProcessFunction());
        process.print();


        env.execute("windowApi");
    }
}
