package cn.wonhigh.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val lineRdd: RDD[String] = sc.textFile("E:/686/personal/Demo.java", 1)
    val wordsRdd: RDD[String] = lineRdd.flatMap(line => line.split(" "))
    val wordValRdd: RDD[(String, Int)] = wordsRdd.map(word => (word, 1))
    val reduceRes: RDD[(String, Int)] = wordValRdd.reduceByKey(_ + _)
    val reverseRes: RDD[(Int, String)] = reduceRes.map(t => (t._2, t._1))
    val value: RDD[(Int, String)] = reverseRes.sortByKey(false)
    println(value)
  }
}
