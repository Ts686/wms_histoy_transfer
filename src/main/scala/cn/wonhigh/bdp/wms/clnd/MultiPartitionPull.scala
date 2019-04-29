package cn.wonhigh.bdp.wms.clnd

import org.apache.spark.sql.SparkSession

object MultiPartitionPull {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ReadOracle")
      .master("local[2]")
      //  .enableHiveSupport()
      .getOrCreate()
    val url = "jdbc:mysql://10.234.6.43:3306/retail_pos"
    val tableName = "order_main"
    val prop = new java.util.Properties
    prop.setProperty("user", "usr_super")
    prop.setProperty("password", "usr_super")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    val jdbcDF = spark.read.jdbc(url, tableName, prop)
    jdbcDF.createOrReplaceTempView("order_main")
    spark.sql("select count(0) from order_main where update_time>=cast('2012-10-01 12:00:00' as timestamp)").explain()
  }
}
