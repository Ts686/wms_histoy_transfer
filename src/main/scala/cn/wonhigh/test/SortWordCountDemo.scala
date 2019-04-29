package cn.wonhigh.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * wordCount 程序 scala版本
  *
  */
object SortWordCountDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    //读取目标文件
    val lineRdd: RDD[String] = sc.textFile("E:/686/personal/Demo.java", 1)
    //压平
    val wordsRdd: RDD[String] = lineRdd.flatMap(line => line.split(" "))
    //转换为tuple
    val wordValRdd: RDD[(String, Int)] = wordsRdd.map(word => (word, 1))
    //按照key聚合
    val reduceRes: RDD[(String, Int)] = wordValRdd.reduceByKey(_ + _)
    //交换key-value位置
    val reverseRes: RDD[(Int, String)] = reduceRes.map(t => (t._2, t._1))
    //按照出现次数排序
    val sortRes: RDD[(Int, String)] = reverseRes.sortByKey(false)
    //互换key-value位置
    val res: RDD[(String, Int)] = sortRes.map(t => (t._2, t._1))
    //打印
    res.foreach(t => println(t._1.trim + " appear " + t._2 + " times ."))
  }
}
