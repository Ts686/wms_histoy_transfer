package cn.wonhigh.exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SecondarySortDemo {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("E:/686/personal/Demo.txt")
    val mapRdd: RDD[(SecondarySortKey, String)] = lines.map(line => {
      val splits: Array[String] = line.split(" ")
      (new SecondarySortKey(splits(0).toInt, splits(1).toInt), line)
    })
    //排序
    val sortRdd: RDD[(SecondarySortKey, String)] = mapRdd.sortByKey()
    val res: RDD[String] = sortRdd.map(t => (t._2))
    println(res)

  }
}
