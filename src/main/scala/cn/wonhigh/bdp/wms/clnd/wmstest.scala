package cn.wonhigh.bdp.wms.clnd

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

object wmstest {
  def main(args: Array[String]): Unit = {
    var s = "hello"
    println(s.reverse.toBuffer.size)
    if (args.length != 2) {
      println("请输入文件目录和Appname")
      return
    }
    val appname = args(1)
    val spark = getSparkSession(appname)
    spark.sparkContext.setLogLevel("ERROR")
    val tablefilename = args(0)
    val tableFileName = "/data/wonhigh/wms_histoy_transfer/tables/wms_tables_0402_1"
    val tablelist = Source.fromFile(tableFileName).getLines().toList
    //隐士转换
    println(tablelist)
    for (table_name <- tablelist) {
      val tablename: Array[String] = table_name.split(",")
      val name = tablename(0).replaceAll("gyl_wms_city_", "").replaceAll("_tmp", "")
      println(tablename.reverse.toBuffer)
      println(table_name)
      val oracleDriverUrl = "jdbc:oracle:thin:@//10.240.4.156:1521/ORCL"
      val user = "usr_bdp"
      val password = "usr_bdp_123"
      val driver = "oracle.jdbc.driver.OracleDriver"
      val oracleDriverUrl_202 = "jdbc:oracle:thin:@//10.240.3.202:1521/HD_ASP_002"
      val oracleDriverUrl_204 = "jdbc:oracle:thin:@//10.240.3.204:1521/LY_ASP_002"
      val oracleDriverUrl_206 = "jdbc:oracle:thin:@//10.240.4.206:1521/WMS_O2O_002"
      val oracleDriverUrl_215 = "jdbc:oracle:thin:@//10.240.4.215:1521/WMS_DS_002"
      val user_online = "dc_wms"
      val password_online = "fv+edjisfpmg"
      val dbtable = "usr_wms_city." + name.replace("_old", "")
      println(dbtable)
      val online_talbe_202 = getDataFrame(oracleDriverUrl_202, user_online, password_online, dbtable, driver, spark).withColumn("spark_isonline", lit("1"))
      val online_talbe_204 = getDataFrame(oracleDriverUrl_204, user_online, password_online, dbtable, driver, spark).withColumn("spark_isonline", lit("1"))
      val online_talbe_206 = getDataFrame(oracleDriverUrl_206, user_online, password_online, dbtable, driver, spark).withColumn("spark_isonline", lit("1"))
      val online_talbe_215 = getDataFrame(oracleDriverUrl_215, user_online, password_online, dbtable, driver, spark).withColumn("spark_isonline", lit("1"))
      var all_df: DataFrame = online_talbe_202.unionAll(online_talbe_204).unionAll(online_talbe_206).unionAll(online_talbe_215)
      spark.sql("drop table if EXISTS " + "bdp_tmp.gyl_wms_city_" + name + "_tmp")
      println("开始处理" + name)
      all_df.cache()
      //all_df.createOrReplaceTempView("aaa")
      // spark.sql("insert overwrite table bdp_tmp.gyl_wms_city_bill_im_import_tmp as select * from aaa")
      all_df.write.saveAsTable("bdp_tmp.gyl_wms_city_" + name + "_tmp")
      val a = online_talbe_202.count() + online_talbe_204.count() + online_talbe_206.count() + online_talbe_215.count()
      val b = spark.sql("select 1 from  " + "bdp_tmp.gyl_wms_city_" + name + "_tmp").count()
      println(a)
      println(b)
      println(all_df.count())
      if (a == b) println("结束处理" + name) else println("结果异常" + name)
    }
  }

  def getSparkSession(appname: String) = {
    SparkSession.builder().master("yarn").appName(appname).enableHiveSupport().getOrCreate()
  }

  def getDataFrame(oracleDriverUrl: String, user_online: String, password_online: String, dbtable: String, driver: String, spark: SparkSession): DataFrame = {
    val hd_usr_wms_city_2014 = Map("url" -> oracleDriverUrl,
      "user" -> user_online,
      "password" -> password_online,
      "dbtable" -> dbtable,
      "driver" -> driver)
    val DF: DataFrame = spark.read.options(hd_usr_wms_city_2014).format("jdbc").load()
    DF
  }
}