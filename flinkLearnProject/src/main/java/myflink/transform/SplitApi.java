package myflink.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * 分流API ，已经过时
 *
 * @Author jiahao
 * @Date 2020/4/20 19:29
 */
public class SplitApi {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.readTextFile("D:\\flink\\flinkLearnProject\\src\\main\\resources\\testfile");

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = dataStreamSource.map(each -> {
            String[] split = each.split(",");
            return Tuple2.of(split[0], Integer.valueOf(split[1]));
        }).returns(Types.TUPLE(Types.STRING,Types.INT));
        // 分流
        SplitStream<Tuple2<String, Integer>> splitStream = map.split(new OutputSelector<Tuple2<String, Integer>>() {
            @Override
            public Iterable<String> select(Tuple2<String, Integer> value) {
                List<String> res = new ArrayList<>();
                if (value.f0.equals("wushijia")) {
                    res.add(value.f0);
                    return res;
                } else {
                    res.add(value.f0);
                }
                return res;
            }
        });
        DataStream<Tuple2<String, Integer>> wushijia =
                splitStream.select("wushijia");
        DataStream<Tuple2<String, Integer>> shijia = splitStream.select("shijia");

        // connect 合并两个流 两个数据结构可以不一样 但是最多两条流
        SingleOutputStreamOperator<Tuple2<Integer, String>> map1 = wushijia.map(each -> Tuple2.of(each.f1,each.f0)).returns(Types.TUPLE(Types.INT,Types.STRING));
        ConnectedStreams<Tuple2<Integer, String>, Tuple2<String, Integer>> connect = map1.connect(shijia);
        SingleOutputStreamOperator<String> map2 = connect.map(new CoMapFunction<Tuple2<Integer, String>, Tuple2<String, Integer>, String>() {
            @Override
            public String map1(Tuple2<Integer, String> value) throws Exception {
                return value.f0 + "" + System.currentTimeMillis();
            }

            @Override
            public String map2(Tuple2<String, Integer> value) throws Exception {
                return value.f0 + System.currentTimeMillis();
            }
        });
        map2.print();
        // union 合并多个流，两个流数据结构必须一致，但是可以合并多个流
        DataStream<Tuple2<String, Integer>> union = wushijia.union(shijia);
        union.print("union");
//        sp1.print("wushijia");
        env.execute("Split");

    }

}
