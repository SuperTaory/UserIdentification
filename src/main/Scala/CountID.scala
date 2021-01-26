import org.apache.spark.sql.SparkSession

object CountID {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("CountID")
            .getOrCreate()
        val sc = spark.sparkContext

        val IDMap = sc.textFile(args(0)).map(line => (line.split(',')(0).drop(1), 1))

        val result = IDMap.reduceByKey(_ + _).filter(_._2 > 10).repartition(1).sortBy(_._2, ascending = false)

        result.saveAsTextFile(args(1))
        sc.stop()
    }
}
