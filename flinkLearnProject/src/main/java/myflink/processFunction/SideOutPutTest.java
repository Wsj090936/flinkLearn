package myflink.processFunction;

import myflink.windowApi.TimeWindowTest;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

import static sun.misc.Version.print;

/**
 * 侧输出流测试
 *
 * @Author jiahao
 * @Date 2020/5/28 21:43
 */
public class SideOutPutTest {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 7777, "\n");

        dataStream.print();

        SingleOutputStreamOperator<TimeWindowTest.WindowPerson> process = dataStream.map(new TimeWindowTest.PersonMapFunction()).
                process(new YoungAlert());

        process.getSideOutput(new OutputTag<String>("shijia_alert"){}).print();

        env.execute("SideOutPut");
    }


}

class YoungAlert extends ProcessFunction<TimeWindowTest.WindowPerson, TimeWindowTest.WindowPerson> {

    private final OutputTag<String> outputTag = new OutputTag<String>("shijia_alert"){};
    @Override
    public void processElement(TimeWindowTest.WindowPerson value, Context ctx, Collector<TimeWindowTest.WindowPerson> out) throws Exception {
        if(!value.getName().equals("shijia")){
            // 如果名字不包含shijia 就输出到侧输出流
            ctx.output(outputTag, "检测到非法用户:" + value.getName());
        }else {
            out.collect(value);
        }
    }
}
