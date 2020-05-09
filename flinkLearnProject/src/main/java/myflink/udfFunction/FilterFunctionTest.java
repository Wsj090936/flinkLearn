package myflink.udfFunction;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * fliterFunceion自定义
 *
 * @Author jiahao
 * @Date 2020/5/9 20:16
 */
public class FilterFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.readTextFile("D:\\flink\\flinkLearnProject\\src\\main\\resources\\testfile");

        SingleOutputStreamOperator<MyPerson> map = dataStreamSource.map(new MapFunction<String, MyPerson>() {
            @Override
            public MyPerson map(String s) throws Exception {
                String[] split = s.split(",");
                MyPerson person = new MyPerson();
                person.setName(split[0]);
                person.setAge(Integer.valueOf(split[1]));
                return person;
            }
        }).returns(MyPerson.class);
        SingleOutputStreamOperator<MyPerson> filter = map.filter(new MyPersonFilter());
        filter.print();
        env.execute("filterFunction");
    }
    @Data
    public static class MyPerson{
        private String name;
        private Integer age;
    }
    static class MyPersonFilter implements FilterFunction<FilterFunctionTest.MyPerson>{

        @Override
        public boolean filter(MyPerson myPerson) throws Exception {
            return myPerson.getAge() > 10;
        }
    }
}
