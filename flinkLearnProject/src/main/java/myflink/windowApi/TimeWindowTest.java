package myflink.windowApi;

import lombok.Data;
import myflink.udfFunction.FilterFunctionTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * 时间窗口测试
 *
 * @Author jiahao
 * @Date 2020/5/17 12:03
 */
public class TimeWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);// 方便测试，设置全局并行度为1
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);// 指定时间语义为eventTime
        DataStreamSource<String> dataStreamSource =
                env.socketTextStream("localhost", 7777, "\n");
        // 从socket中读取数据
        SingleOutputStreamOperator<WindowPerson> minTimeStream = dataStreamSource.map(new PersonMapFunction())
//                .assignTimestampsAndWatermarks(new MyWaterMarks())//
                .assignTimestampsAndWatermarks(new MyBound(Time.seconds(10)))
                .keyBy(new MyKeySelector())
                .timeWindow(Time.seconds(10))
//                .timeWindow(Time.seconds(10),Time.seconds(5))// 设置滑动窗口，5S滑动
                .reduce(new MyReduceFunction());// 设置时间窗口10S，注意，这里是process Time
        dataStreamSource.print();

        minTimeStream.print();


        env.execute("windowApi");
    }

    @Data
    public static class WindowPerson{
        private Long id;
        private String name;
        private Long eventTimeStamp;
    }
    public static class PersonMapFunction implements MapFunction<String,WindowPerson> {
        @Override
        public WindowPerson map(String eachLine) throws Exception {

            String[] split = eachLine.split(",");
            WindowPerson person = new WindowPerson();
            person.setId(Long.valueOf(split[0]));
            person.setName(split[1]);
            person.setEventTimeStamp(Long.valueOf(split[2]));
            return person;
        }
    }

    public static class  MyKeySelector implements KeySelector<WindowPerson,String>{

        @Override
        public String getKey(WindowPerson value) throws Exception {
            return value.getName();
        }
    }
    static class MyReduceFunction implements ReduceFunction<WindowPerson>{

        @Override
        public WindowPerson reduce(WindowPerson last, WindowPerson now) throws Exception {
            // 取每一个name下，最小的id
            WindowPerson person = last.getId() < now.getId() ? last : now;
            return person;
        }
    }
    // 自定义水位线
    static class MyWaterMarks implements AssignerWithPeriodicWatermarks<WindowPerson>{
        int bound = 10000;// 10S
        Long maxT = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {// 获得当前水位线 = 最大时间 - 延迟时间bound
            return new Watermark(maxT - bound);
        }

        @Override
        public long extractTimestamp(WindowPerson windowPerson, long l) {
            maxT = Math.max(maxT, windowPerson.getEventTimeStamp() * 1000);
            return windowPerson.getEventTimeStamp() * 1000;
        }
    }

    /**
     * 类似于 MyWaterMarks 的实现
     */
    static class MyBound extends BoundedOutOfOrdernessTimestampExtractor<WindowPerson>{

        public MyBound(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(WindowPerson element) {
            return element.getEventTimeStamp() * 1000;
        }
    }
}
