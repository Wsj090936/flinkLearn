package myflink.state;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * TODO
 *
 * @Author jiahao
 * @Date 2020/5/31 12:34
 */
public class PersonKeySelector implements KeySelector<Person,String> {
    @Override
    public String getKey(Person value) throws Exception {
        return value.getName();
    }
}
