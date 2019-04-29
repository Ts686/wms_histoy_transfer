package cn.wonhigh.bdp.wms.clnd

import org.apache.spark.sql.SparkSession

object OracleTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ReadOracle")
      .master("local[2]")
      //  .enableHiveSupport()
      .getOrCreate()
    val url = "jdbc:oracle:thin:@//10.240.3.202:1521/HD_ASP_002"
    val tableName = "usr_wms_city.bill_om_outstock"
    val prop = new java.util.Properties
    prop.setProperty("user","dc_wms")
    prop.setProperty("password","fv+edjisfpmg")
    prop.setProperty("driver","oracle.jdbc.driver.OracleDriver")
    val jdbcDF = spark.read.jdbc(url,tableName,prop)
    jdbcDF.createOrReplaceTempView("bill_om_outstock")
    spark.sql("select count(0) from bill_om_outstock where CREATETM>cast('2019-03-18 17:00:00' as timestamp)").show
  }
}
