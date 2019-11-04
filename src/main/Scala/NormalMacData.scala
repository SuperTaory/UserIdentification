import org.apache.spark.{SparkConf, SparkContext}

object NormalMacData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NormalMacData")
    val sc = new SparkContext(conf)

    val macFile = sc.textFile(args(0)).map(line => {
      val fields = line.split(',')
      val macId = fields(0).drop(1)
      val time = fields(1)
      val station = fields(2).dropRight(1)
      (macId, (time, station))
    })

    val groupedMacData = macFile.groupByKey().filter(v => v._2.size > 5 && v._2.size < 3000).mapValues(_.toList)

    val flattenMacData = groupedMacData.flatMap(line => {
      for (v <- line._2) yield {
        (line._1, v._1, v._2)
      }
    })

//    val sortedMacData = flattenMacData.sortBy(x => (x._1, x._2), ascending = true)

    flattenMacData.saveAsTextFile(args(1))
    sc.stop()
  }
}
