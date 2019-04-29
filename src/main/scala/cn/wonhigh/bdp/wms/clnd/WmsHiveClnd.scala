package cn.wonhigh.bdp.wms.clnd

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * 清洗hive中相关表，将数据还原到dc_ods中
  */
object WmsHiveClnd {
  //hive关键字
  var keys:List[String] = null

  def main(args: Array[String]): Unit = {
    if(args.length !=2) {
      println("输入配置文件目录和AppName")
      return
    }
    val tableFileName = args(0)
    var dir = tableFileName.substring(0,tableFileName.lastIndexOf("/"))
    dir=dir.substring(0,dir.lastIndexOf("/"))
    //后去hive的关键字放到
    val keyFiles = Source.fromFile(s"${dir}/hive_keywords.txt")
    if(null ==keys) keys = keyFiles.getLines().toList
    val tablePropertys = Source.fromFile(tableFileName).getLines()
      .map(line => {
        val tables = line.split(",")
        val tableName = tables(0)
        (tableName.substring(0,tableName.lastIndexOf("_")), tables(1), tables(2))
      }).toList
    val spark = getSparkSession(args(1))
    spark.sparkContext.setLogLevel("ERROR")
    //需要跳过的表（已处理的表）
    val wms_clnd_finish = s"dc_ods.wms_clnd_finish_${args(1)}"
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    import spark.implicits._
    import org.apache.spark.sql.AnalysisException
    try {
      spark.table(wms_clnd_finish)
    }catch {
      case e:AnalysisException=>{
        //先插入一条测试数据
        Seq(("test", 10l, sdf.format(new Date())))
          .toDF("tableName","totCount","createTime")
          .write.mode("append").saveAsTable(s"${wms_clnd_finish}")
      }
    }
    val skipTables = spark.sql(s"select tablename,totcount from ${wms_clnd_finish} where createTime='${sdf.format(new Date())}'").rdd.map({
      case org.apache.spark.sql.Row(tableName, totCount) => tableName.toString
    }).collect().toList
    wmsCldn(spark, tablePropertys.filter(a => false == skipTables.contains(a._1)),args(1))
    println("job_success")
  }

  /**
    * 清洗相关表
    *
    * @param spark
    * @param tablePropertys
    * @param skipTables
    */
  def wmsCldn(spark: SparkSession, tablePropertys: List[(String, String, String)],appName:String) = {
    var count=1
    tablePropertys.foreach {
      case (tableName, pks, partition) => {
        import spark.implicits._
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val startTime = System.currentTimeMillis()
        val preff = s"[${count}/${tablePropertys.size}]"
        println(s"${new Date().toString} ${preff} 正在执行的表为：${tableName}")
        //获取对应的列
        val columns = spark.sql(s"select * from dc_ods.${tableName}_ods limit 1")
          .schema.map(a=>{
          //处理hive关键字
          if(keys.contains(a.name.replace("_",""))) a.name.replace("_","") else a.name
        })
          .mkString(",")
        //先删除表数据
        spark.sql(s"truncate table dc_ods.${tableName}_ods").show()
        //如果没有配置主键则不用去重,知道导入ods即可
        var sql=""
        //判断是否需要排序
        var partitionSql= s" date_format(nvl(${partition},'2017-01-01'),'yyyyMM') AS partition_date "
        //是否需要orderBy
        var orderBySql=s" ORDER BY nvl(${partition},to_date('2017-01-01', 'yyyy-MM-dd'))  DESC"
        if(null ==partition || "".equals(partition) || "null".equals(partition)){
            partitionSql=s" date_format('${sdf.format(new Date())}','yyyyMM') AS partition_date  "
            orderBySql=" ORDER BY 1 "
        }
        if(null ==pks || "".equals(pks) || "null".equals(pks)){
          sql= s" INSERT overwrite TABLE dc_ods.${tableName}_ods partition(partition_date)  " +
            s" select ${columns} from ( select *,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS ods_update_time," +
            partitionSql +
            s" from bdp_tmp.${tableName}_tmp ) main where 1=1 "
        }else {
          val pk = pks.replace(":", ",")
          sql = s" INSERT overwrite TABLE dc_ods.${tableName}_ods partition(partition_date)  " +
            s" select ${columns} from ( select *,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') AS ods_update_time," +
            partitionSql +
            s" , row_number() over(PARTITION BY ${pk}  ${orderBySql}) rn" +
            s" from bdp_tmp.${tableName}_tmp ) main where rn=1 "
        }
        println(s"${new Date().toString} ${preff} 解析的sql为：${sql}")
        spark.sql(sql).show()
        val endTime =  System.currentTimeMillis()
        Seq((tableName, endTime - startTime, sdf.format(new Date())))
          .toDF("tableName","totCount","createTime")
          .write.mode("append").saveAsTable(s"dc_ods.wms_clnd_finish_${appName}")
        println(s"${new Date().toString} ${preff} ${tableName}完成，耗时：${endTime-startTime}")
        count=count+1
      }
    }
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
