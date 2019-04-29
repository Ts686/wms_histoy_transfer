package excercise;

import cn.wonhigh.exercise.SecondarySortKey;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 二次排序，如果第一个key相等，就按第二个
 */
public class SecondarySortDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("E:/686/personal/Demo.txt");
        JavaPairRDD<SecondarySortKey, String> mapRdd = lines.mapToPair(
                new PairFunction<String, SecondarySortKey, String>() {
                    @Override
                    public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
                        String[] split = line.split(" ");
                        SecondarySortKey secondarySortKey = new SecondarySortKey
                                (Integer.valueOf(split[0]), Integer.valueOf(split[1]));
                        return new Tuple2<SecondarySortKey, String>(secondarySortKey, line);
                    }
                });
        //排序 通过自定义的排序规则来排序 SecondarySortKey
        JavaPairRDD<SecondarySortKey, String> sortRdd = mapRdd.sortByKey();
        sortRdd.foreach(new VoidFunction<Tuple2<SecondarySortKey, String>>() {
            @Override
            public void call(Tuple2<SecondarySortKey, String> t) throws Exception {
                System.out.println(t._2);
            }
        });
    }
}
