package cn.wonhigh.bdp.wms.clnd

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.io.Source

object WmsPullTools {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("输入配置文件目录")
      return
    }
    val spark = getSparkSession(args(1))
    spark.sparkContext.setLogLevel("ERROR")
    val tableFileName = args(0)
    val tablelist = Source.fromFile(tableFileName).getLines().toList
    var count = 1
    for (table_name <- tablelist) {
      val table_names = table_name.split(",")
      val preff = s"[${count}/${tablelist.size}]"
      val name = table_names(0).replaceAll("gyl_wms_city_", "").replaceAll("_tmp", "")
      println(s"${new Date().toString} ${preff} 正在执行的表为：${name}")
      connectOracle(name, spark, table_names(3), table_names(4), table_names(5), table_names(6))
      println(s"${new Date().toString} ${preff} 处理完成的表为：${name}")
      count = count + 1
    }
    println("job_success")
  }

  /**
    * 连接oracle出去数据
    *
    * @param table_name
    * @param spark
    * @param onlineOrHistory
    * @param partitionName
    * @param lowerBound
    * @param upperBound
    */
  def connectOracle(table_name: String, spark: SparkSession, onlineOrHistory: String, partitionName: String, lowerBound: String, upperBound: String): Unit = {
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
    val dbtable = "usr_wms_city_jk." + table_name.replace("_old", "")
    val online_talbe_202 = getDataFrame(oracleDriverUrl_202, user_online, password_online, dbtable, driver, spark, partitionName, lowerBound, upperBound)
    val online_talbe_204 = getDataFrame(oracleDriverUrl_204, user_online, password_online, dbtable, driver, spark, partitionName, lowerBound, upperBound)
    val online_talbe_206 = getDataFrame(oracleDriverUrl_206, user_online, password_online, dbtable, driver, spark, partitionName, lowerBound, upperBound)
    val online_talbe_215 = getDataFrame(oracleDriverUrl_215, user_online, password_online, dbtable, driver, spark, partitionName, lowerBound, upperBound)

    val online_talbe_202_columns: Array[String] = online_talbe_202.columns

    var all_df: DataFrame = null

    /**
      * 处理历史数据
      */
    def historyDataFrame = {
      val file = Source.fromFile("/data/wonhigh/dc/shell/wms/table_databases.txt")
      val databases = new ListBuffer[String]
      for (line <- file.getLines()) {
        if (line.split(",")(1).equals(table_name)) {
          databases.append(line.split(",")(0))
        }
      }
      for (database <- databases) {
        val database_table = database + "." + table_name
        //val oracle_df = get_oracle_dataframe(oracleDriverUrl, user, password, database_table, driver, spark)
        val oracle_df = getDataFrame(oracleDriverUrl, user, password, database_table, driver, spark, partitionName, lowerBound, upperBound)
        val compare_df = compare_colums(online_talbe_202_columns, oracle_df, spark)
        if (null == all_df) all_df = compare_df else all_df = all_df.unionAll(compare_df)
      }
    }

    /**
      * 在线数据
      */
    def onlineDataFrame = {
      all_df = online_talbe_202
        .unionAll(online_talbe_204)
        .unionAll(online_talbe_206)
        .unionAll(online_talbe_215)
    }

    /**
      * onlineOrHistory：1 在线数据 2 历史数据 3 全部数据
      */
    onlineOrHistory match {
      case "1" => onlineDataFrame
      case "2" => {
        historyDataFrame
      }
      case "3" => {
        onlineDataFrame
        historyDataFrame
      }
    }
    spark.sql("drop table if EXISTS " + "bdp_tmp.gyl_wms_city_" + table_name + "_tmp").show()
    all_df.write.mode("append").saveAsTable("bdp_tmp.gyl_wms_city_" + table_name + "_tmp")
  }

  def compare_colums(full_columns: Array[String], compare_df: DataFrame, spark: SparkSession) = {
    val compare_columns: Array[String] = compare_df.columns
    var common_colums = new ListBuffer[String]
    for (i <- 0 until full_columns.size) {
      if (compare_columns.contains(full_columns(i))) {
        common_colums.append(full_columns(i))
      } else {
        common_colums.append("'null' as " + full_columns(i))
      }
    }
    compare_df.createOrReplaceTempView("aaa")
    val return_df = spark.sql("select " + common_colums.mkString(",") + " from aaa")
    //    return_df.write.saveAsTable("bdp_tmp."+databases+"_"+table_name)
    return_df
  }

  /**
    * 获取dataframe
    *
    * @param oracleDriverUrl
    * @param user
    * @param password
    * @param dbtable
    * @param drive
    * @param spark
    * @param partitionName
    * @param lowerBound
    * @param upperBound
    */
  def getDataFrame(oracleDriverUrl: String, user: String, password: String, dbtable: String, drive: String,
                   spark: SparkSession, partitionName: String, lowerBound: String, upperBound: String) = {
    //如果没有分区字段，全表拉
    if (null == partitionName || "".equals(partitionName) || "null".equals(partitionName)) {
      get_oracle_dataframe(oracleDriverUrl, user, password, dbtable, drive, spark)
    } else getOracleDataFrameByPartition(oracleDriverUrl, user, password, dbtable, drive, spark, partitionName, lowerBound, upperBound)
  }

  def get_oracle_dataframe(oracleDriverUrl: String, user: String, password: String, dbtable: String, drive: String, spark: SparkSession) = {
    val hd_usr_wms_city_2014 = Map("url" -> oracleDriverUrl,
      "user" -> user,
      "password" -> password,
      "dbtable" -> dbtable,
      "driver" -> drive)
    val DF: DataFrame = spark.read.options(hd_usr_wms_city_2014).format("jdbc").load()
    DF
  }

  /**
    * @param oracleDriverUrl
    * @param user
    * @param password
    * @param dbtable
    * @param drive
    * @param spark
    * @param partitionName
    * @param lowerBound
    * @param upperBound
    */
  def getOracleDataFrameByPartition(oracleDriverUrl: String, user: String, password: String, dbtable: String, drive: String,
                                    spark: SparkSession, partitionName: String, lowerBound: String, upperBound: String) = {
    val prop = new java.util.Properties
    prop.setProperty("user", user)
    prop.setProperty("password", password)
    prop.setProperty("driver", drive)
    println(s"操作表为：${dbtable}")
    val predicates = getMonthPartitions(partitionName, lowerBound, upperBound)
    spark.read.jdbc(oracleDriverUrl, dbtable, predicates, prop)
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


  /**
    * 计算2个日期之间相差的  以年、月、日为单位，各自计算结果是多少
    * 比如：2011-02-02 到  2017-03-02
    * 以年为单位相差为：6年
    * 以月为单位相差为：73个月
    * 以日为单位相差为：2220天
    *
    * @param fromDate
    * @param toDate
    * @return
    */
  def dayCompare(fromDate: Date, toDate: Date) = {
    import java.util.Calendar
    val from = Calendar.getInstance
    from.setTime(fromDate)
    val to = Calendar.getInstance
    to.setTime(toDate)
    //只要年月
    val fromYear = from.get(Calendar.YEAR)
    val fromMonth = from.get(Calendar.MONTH)
    val toYear = to.get(Calendar.YEAR)
    val toMonth = to.get(Calendar.MONTH)
    toYear * 12 + toMonth - (fromYear * 12 + fromMonth)
  }

  def getMonthPartitions(partitionName: String, lowerBound: String, upperBound: String) = {
    val sdf = new SimpleDateFormat("yyyy/MM/dd")
    val months = dayCompare(sdf.parse(lowerBound), sdf.parse(upperBound))
    println(s"分区数为：${months}")

    /**
      * 月份+1
      *
      * @param a
      * @param month
      */
    def addMonth(a: String, month: Int) = {
      import java.util.Calendar
      val from = Calendar.getInstance
      from.setTime(sdf.parse(a))
      from.add(Calendar.MONTH, month)
      sdf.format(from.getTime)
    }

    (("1970/01/01" -> lowerBound) :: (0 until months).map(a => {
      addMonth(lowerBound, a) -> addMonth(lowerBound, a + 1)
    }).toList).map({
      case (start, end) => s" ${partitionName}>to_date('$start','yyyy/mm/dd ') " + s" AND ${partitionName} <= to_date( '$end','yyyy/mm/dd')"
    }).toArray
  }
}
