package myflink.state;

import lombok.Data;
import myflink.windowApi.TimeWindowTest;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * 状态编程
 *
 * @Author jiahao
 * @Date 2020/5/31 11:57
 */
public class StateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(100000);
        env.getCheckpointConfig().setCheckpointInterval(30000);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 7777, "\n");

        SingleOutputStreamOperator<Person> flatMapSource = dataStreamSource.map(new PersonFunction()).keyBy(new PersonKeySelector())
                .flatMap(new ValueFlatMapFunction(5));

        dataStreamSource.print("data stream");
        flatMapSource.print("flast Map");
        env.execute("stateTest");

    }
}

