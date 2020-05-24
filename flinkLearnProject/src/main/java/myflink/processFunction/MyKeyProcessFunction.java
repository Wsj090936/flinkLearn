package myflink.processFunction;

import myflink.windowApi.TimeWindowTest;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 *
 * @Author jiahao
 * @Date 2020/5/24 19:42
 */
public class MyKeyProcessFunction extends KeyedProcessFunction<String, TimeWindowTest.WindowPerson, String> {
    ValueState<String>  nameState = null;
    ValueState<Long> currentTimer = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化一些变量
        nameState = getRuntimeContext().getState(new ValueStateDescriptor<>("nameState", String.class));
        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("timeState", Long.class));
    }


    @Override
    public void processElement(TimeWindowTest.WindowPerson value, Context ctx, Collector<String> out) throws Exception {

        String curName = value.getName();

        // 上一个name
        String preName = nameState.value();
        // 更新name
        nameState.update(curName);
        // 如果名字相同，并且没有设置过定时器，则设置
        if(currentTimer.value() == null || currentTimer.value() == 0){
            // 设置定时器
            long curTime = ctx.timerService().currentProcessingTime() + 10000;
            ctx.timerService().registerProcessingTimeTimer(curTime);
            currentTimer.update(curTime);
        }else if(!curName.equals(preName) || StringUtils.isEmpty(preName)){
            // 第一条数据或者名字不相同，需要清除
            ctx.timerService().deleteProcessingTimeTimer(currentTimer.value());
            currentTimer.clear();
        }


    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        out.collect(ctx.getCurrentKey() + "_" + "名字相同");
    }
}
