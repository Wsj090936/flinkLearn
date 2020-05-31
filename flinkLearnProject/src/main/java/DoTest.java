import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * TODO
 *
 * @Author jiahao
 * @Date 2020/5/25 9:28
 */
public class DoTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        List<Integer> list = new ArrayList<Integer>(){
            {
                add(1);
            }
        };
        System.out.println(list.get(0));
    }


}
