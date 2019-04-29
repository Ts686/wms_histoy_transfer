import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Persisent {
    public <T> T show(T t) {
        return t;
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("hello");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> fileRdd = sc.textFile("E:/686/personal/Demo.java");
        System.out.println("文件中取出共：" + fileRdd.count());
        JavaRDD<String> flatRes = fileRdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        System.out.println("压平后共：" + flatRes.count());
        JavaPairRDD<String, Integer> mapPairRes = flatRes.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });
        System.out.println("tuple后共：" + mapPairRes.count());

        JavaPairRDD<String, Integer> wordCountRes = mapPairRes.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println("统计结果为:" + wordCountRes.count());

        JavaPairRDD<Integer, String> wcResRevers = wordCountRes.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> wcRes) throws Exception {

                return new Tuple2<Integer, String>(wcRes._2, wcRes._1);
            }
        });
        JavaPairRDD<Integer, String> sortRes = wcResRevers.sortByKey(false);
        JavaRDD<Tuple2> map = sortRes.map(new Function<Tuple2<Integer, String>, Tuple2>() {
            @Override
            public Tuple2 call(Tuple2<Integer, String> v1) throws Exception {
                return new Tuple2<String, Integer>(v1._2, v1._1);
            }
        });
        System.out.println(sortRes);

    }
}
