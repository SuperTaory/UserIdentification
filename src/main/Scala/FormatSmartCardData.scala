import org.apache.spark.{SparkConf, SparkContext}

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
      (scID, time, stationName, tag)
    })

    val groupedData = originalData.sortBy(x => (x._1, x._2))

    groupedData.saveAsTextFile(args(1))
    sc.stop()
  }
}
