import org.apache.spark.{SparkConf, SparkContext}

object CountPassengerFlow {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CountPassengerFlow")
    val sc = new SparkContext(conf)

    val ODFile = sc.textFile(args(0))

    val ODFileRDD = ODFile.filter(_.split(',').length == 8).map(line => {
      val fields = line.split(',')
      val pid = fields(1)
      val time = fields(4).replace('T', ' ').dropRight(5)
      val station = fields(6)
      (pid, time, station)
    })

    // 统计每个站点的客流量,按照客流量降序排序并保存
    val PassengerFlowOfStation = ODFileRDD.map(x => (x._3, 1)).reduceByKey(_+_).sortBy(_._2, ascending = false)
    PassengerFlowOfStation.repartition(1).saveAsTextFile(args(1))

    sc.stop()
  }
}
