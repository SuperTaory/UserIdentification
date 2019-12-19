import org.apache.spark.{SparkConf, SparkContext}
import GeneralFunctionSets.{transTimeToTimestamp, transTimeToString}
import scala.collection.mutable.ListBuffer

object FormatSmartCardData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FormatSmartCardData")
    val sc = new SparkContext(conf)

//    val incompleteData = sc.textFile(args(0)).filter(line => line.split(',').length < 7)

    val originalData = sc.textFile(args(0)).map(line => {
      val fields = line.split(',')
      val scID = fields(1)
      var tag = fields(3)
      var time = fields(4)
      var stationName = "null"
      if (fields.length >= 7)
        stationName = fields(6)

      //修改时间样式
      time = time.replace('T',' ').dropRight(5)

      //修改进出站标志
      if (tag == "地铁入站")
        tag = "21"
      else
        tag = "22"
      (scID, (transTimeToTimestamp(time), stationName, tag))
    })

    // 去除残缺（进出站不完整）的数据和同站进出的数据
    val processedData = originalData.groupByKey().mapValues(_.toList.sortBy(_._1)).map(line => {
      val data = line._2
      val temp = new ListBuffer[(Long, String, String)]
      var x = 0
      while (x + 1 < data.length) {
        if (data(x)._2 != data(x + 1)._2 && data(x)._3 == "21" && data(x+1)._3 == "22" && data(x+1)._1 - data(x)._1 < 10800) {
          temp.append(data(x))
          temp.append(data(x+1))
          x += 2
        }
        else
          x += 1
      }
      (line._1, temp)
    })

    // 将AFC数据格式化为(684017436,2019-06-21 08:07:13,龙华,22)
    val flattenData = processedData.flatMap(line => {
      for (v <- line._2) yield {
        (line._1, transTimeToString(v._1), v._2, v._3)
      }
    })

    flattenData.saveAsTextFile(args(1))
    sc.stop()
  }
}
