import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object testDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestDemo").setMaster("local")
    val sc = new SparkContext(conf)
    //val sq = SparkSession.builder().config(conf).getOrCreate()
    val sq = new SQLContext(sc)

    val macFile = sq.read.parquet(args(0))
    macFile.printSchema()
    val macSchema = macFile.schema
    //    macFile.createOrReplaceTempView("MacTable")
    macFile.registerTempTable("MacTable")
    // 1559318400是指2019/06/01 00:00:00对应的unix时间戳
    val macRDD = sq.sql("select * from MacTable where " + macSchema.fieldNames(1) + " > 1559318400").rdd

    val transformedMacRDD = macRDD.map(line => {
      val fields = line.toString().split(',')
      val macId = fields(0).drop(1)
      val time = fields(1)
      val station = fields(2)
      (macId, time, station)
    }).sortBy(line => (line._1, line._2), ascending = true)

    transformedMacRDD.saveAsTextFile(args(1))
    sc.stop()
  }
}
