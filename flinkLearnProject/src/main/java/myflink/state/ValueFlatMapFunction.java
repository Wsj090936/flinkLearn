package myflink.state;

import myflink.windowApi.TimeWindowTest;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 状态编程-flatMap
 *
 * @Author jiahao
 * @Date 2020/5/31 12:21
 */
public class ValueFlatMapFunction extends RichFlatMapFunction<Person, Person> {
    private Integer factor;

    public ValueFlatMapFunction() {
        super();
    }
    public ValueFlatMapFunction(Integer factor) {
        super();
        this.factor = factor;
    }

    private ValueState<Integer> lastAge = null;// 注意这里的状态只能用在keyby后面
    
    @Override
    public void open(Configuration parameters) throws Exception {
        lastAge = this.getRuntimeContext().getState(new ValueStateDescriptor<Integer>("last_age",Integer.class));
    }

    @Override
    public void flatMap(Person value, Collector<Person> out) throws Exception {

        Integer age = value.getAge();

        if(lastAge.value() != null){
            Integer last = lastAge.value();

            if(Math.abs(age - last) > factor){
                // 年龄相差过大，就输出
                out.collect(value);
            }
        }
        // 最后更新state
        lastAge.update(age);

    }
}
