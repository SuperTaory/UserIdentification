import org.apache.spark.{SparkConf, SparkContext}
import GeneralFunctionSets.transTimeToString

object MergeAndSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MergeAndSort")
    val sc = new SparkContext(conf)

    // 合并Mac数据文件并按照ID和time排序
    val macFile = sc.textFile(args(0)).map(line => {
      val fields = line.split(',')
      val id = fields(0).drop(1)
      val time = transTimeToString(fields(1).toLong)
      val station = fields(2)
      val dur = fields(3).dropRight(1).toInt
      (id, time, station, dur)
    }).repartition(100)

    val sortedFile = macFile.sortBy(x => (x._1, x._2))

    sortedFile.saveAsTextFile(args(1))
    sc.stop()
  }
}
