package cn.wonhigh.bdp.wms.clnd

import java.util.Date

import org.apache.spark.sql.{SparkSession}

import scala.io.Source

object WmsCheckCount {
  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      println("输入配置文件目录和AppName")
      return
    }
    val tableFileName = args(0)
    val tablePropertys = Source.fromFile(tableFileName).getLines()
      .map(line => {
        val tables = line.split(",")
        val tableName = tables(0)
        (tableName.substring(0, tableName.lastIndexOf("_")), tables(1), tables(2))
      }).toList
    val spark = getSparkSession(args(1))
    spark.sparkContext.setLogLevel("ERROR")
    wmsCheckCount(spark, tablePropertys, args(2), args(3), args(4), args(1))
  }

  /**
    * 检查表数量
    *
    * @param spark
    * @param tablePropertys
    * @param skipTables
    */
  def wmsCheckCount(spark: SparkSession, tablePropertys: List[(String, String, String)], typeName: String, startTime: String, endTime: String, appName: String) = {
    var a: Int = 1
    tablePropertys.foreach {
      case (tableName, pks, partition) => {
        val start = System.currentTimeMillis()
        val preff = s"[${a}/${tablePropertys.size}]"
        println(s"${preff} 正在执行的表为：${tableName}")
        val table_name = tableName.split(",")(0).replaceAll("gyl_wms_city_", "").replaceAll("_tmp", "")
        import spark.implicits._
        val sourceCount = 100l;
        val where = if (!"".equals(startTime) && !"".equals(endTime)) s" where ${partition}>='${startTime}' and ${partition}<'${endTime}' " else "where 1=1 "
        val hiveTableName: String = if ("bdp_tmp".equals(typeName)) s"bdp_tmp.${tableName}_tmp" else s"dc_ods.${tableName}_ods "
        //where条件
        //val hiveWhere = if(!"".equals(startTime) && !"".equals(endTime)) s" where ${partition}>='${startTime}' and ${partition}<${endTime} " else "where 1=1 "
        val hiveCount = spark.sql(s"select count(0) from ${hiveTableName} ${where}").rdd.take(1)(0).getLong(0)
        val resultTableName = if ("bdp_tmp".equals(typeName)) s"bdp_tmp.wms_count_check_${appName}" else s"bdp_tmp.wms_count_ods_check_${appName}"
        Seq((table_name, sourceCount, hiveCount, new Date().toLocaleString)).toDF("tableName", "sourceCount", "hvieCount", "createTime").write.mode("append").saveAsTable(resultTableName)
        val end = System.currentTimeMillis()
        println(s"${preff} ${tableName}完成，耗时：${end - start}")
        a = a + 1
      }
    }
  }


  /**
    * 创建临时表
    *
    * @param oracleDriverUrl
    * @param user
    * @param password
    * @param dbtable
    * @param drive
    * @param spark
    * @return
    */
  def createOracleToSaprk(oracleDriverUrl: String, user: String, password: String, dbtable: String, drive: String, spark: SparkSession, tableName: String) = {
    val hd_usr_wms_city_2014 = Map("url" -> oracleDriverUrl,
      "user" -> user,
      "password" -> password,
      "dbtable" -> dbtable,
      "driver" -> drive)
    spark.read.options(hd_usr_wms_city_2014).format("jdbc").load().createOrReplaceTempView(tableName)
  }

  /**
    * 创建sparkSession
    *
    * @param appName
    * @return
    */
  def getSparkSession(appName: String) = {
    SparkSession.builder()
      .master("yarn")
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()
  }
}
