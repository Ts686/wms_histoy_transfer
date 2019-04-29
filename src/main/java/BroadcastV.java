import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

public class BroadcastV {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("hello");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final int fac = 3;
        final Broadcast<Integer> broadcast = sc.broadcast(fac);
        List<Integer> integers = Arrays.asList(1, 2, 3, 4);
        JavaRDD<Integer> nums = sc.parallelize(integers);
        JavaRDD<Integer> map = nums.map(new Function<Integer, Integer>() {

            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * fac;
            }
        });
        map.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

    }
}
