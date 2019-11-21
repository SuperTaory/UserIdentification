import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

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
      val stationSet : mutable.Set[String] = mutable.Set()
      line._2.foreach(x => stationSet.add(x._2))
      for (v <- line._2) yield {
        (line._1, v._1, v._2, stationSet.size)
      }
    })

    val resultRDD = flattenMacData.filter(_._4 > 1).map(line => (line._1, line._2, line._3))

//    val sortedMacData = flattenMacData.sortBy(x => (x._1, x._2), ascending = true)

    resultRDD.saveAsTextFile(args(1))
    sc.stop()
  }
}
