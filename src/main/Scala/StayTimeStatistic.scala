import org.apache.spark.sql.SparkSession

/**
 * 统计站点停留时长的分布
 */
object StayTimeStatistic {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("Stay-Time Statistic")
            .getOrCreate()

        val sc = spark.sparkContext

        // (00BE3B53C124,2019-06-24 19:13:15,长龙,0)
        val readFile = sc.textFile(args(0)).map(line => {
            val fields = line.split(",")
            val station = fields(2)
            val dur = fields.last.dropRight(1).toLong
            (dur / 10, 1)
        }).reduceByKey(_ + _)
            .repartition(1)
            .sortByKey()
        readFile.saveAsTextFile(args(1))
        spark.stop()
    }
}
