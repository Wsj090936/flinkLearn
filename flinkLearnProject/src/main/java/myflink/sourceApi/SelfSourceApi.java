package myflink.sourceApi;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import scala.math.Ordering;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义数据源 自测时经常用到
 *
 * @Author jiahao
 * @Date 2020/4/19 11:14
 */
public class SelfSourceApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<List<String>> streamSource = env.addSource(new SelfSource());
        streamSource.print();
        env.execute("selfSource");
    }

    static class SelfSource implements SourceFunction<List<String>> {

        private Boolean running = true;

        @Override
        public void run(SourceContext<List<String>> sourceContext) throws Exception {
            List<String> arrayList = new ArrayList<>();
            while (running){
                arrayList.clear();
                // 构造流数据
                int count = 10;
                while (count-- > 0){
                    arrayList.add("count_" + count + System.currentTimeMillis());
                }
                sourceContext.collect(arrayList);
                // 每次睡10S
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }
}
