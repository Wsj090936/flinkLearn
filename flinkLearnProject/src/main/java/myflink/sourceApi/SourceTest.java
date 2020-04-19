package myflink.sourceApi;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * sourceAPI相关使用
 *
 * @Author jiahao
 * @Date 2020/4/15 20:07
 */
public class SourceTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        list.add(5);
        // 这里要设置并行度，不然会乱序
        DataStreamSource<Integer> streamSource = env.fromCollection(list).setParallelism(1);

        streamSource.print();
        env.execute("sourceTest1");


    }

}
