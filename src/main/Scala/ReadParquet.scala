import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

//import org.apache.spark.sql.SQLContext
import GeneralFunctionSets.transTimeToString

object ReadParquet {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReadParquet")
    val sc = new SparkContext(conf)
    val sq = SparkSession.builder().config(conf).getOrCreate()
//    val sq = new SQLContext(sc)

    // 读取mac数据并转换为数据库Table格式
    val macFile = sq.read.parquet(args(0))
    macFile.printSchema()
//    val macSchema = macFile.schema
    macFile.createOrReplaceTempView("MacTable")
//    macFile.registerTempTable("MacTable")
    // 1559318400是指2019/06/01 00:00:00对应的unix时间戳
    val macRDD = sq.sql("select * from MacTable").rdd

    val transformedMacRDD = macRDD.map(line => {
      val fields = line.toString().split(',')
      val macId = fields(0).drop(1)
//      val time = transTimeToString(fields(1).toLong)
//      val station = fields(2)
      val macCount = fields.last.dropRight(1).toInt
      (macId, macCount)
    }).distinct().sortBy(_._2, ascending = false)

    transformedMacRDD.saveAsTextFile(args(1))
    sc.stop()
  }
}
