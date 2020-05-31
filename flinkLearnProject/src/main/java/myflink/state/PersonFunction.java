package myflink.state;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * TODO
 *
 * @Author jiahao
 * @Date 2020/5/31 12:18
 */
public class PersonFunction implements MapFunction<String,Person> {
    @Override
    public Person map(String value) throws Exception {
        String[] split = value.split(",");
        Person person = new Person();
        person.setId(Long.valueOf(split[0]));
        person.setName(split[1]);
        person.setAge(Integer.valueOf(split[2]));

        return person;
    }
}
