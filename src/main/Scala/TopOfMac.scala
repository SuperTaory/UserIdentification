import org.apache.spark.{SparkConf, SparkContext}

object TopOfMac {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TopOfMac")
    val sc = new SparkContext(conf)

    val macFile = sc.textFile(args(0))
    val macRDD = macFile.map(line => {
      val fields = line.split(',')
      val macID = fields(0).drop(1)
      val time = fields(1)
      val station = fields(2).dropRight(1)
      (macID, time, station)
    })

    val countRDD = macRDD.map(x => (x._1, 1)).reduceByKey(_+_).sortBy(_._2, ascending = false)
    countRDD.saveAsTextFile(args(1))

    //val top = countRDD.take(10).map(_._1).toSet

    //val topMacID = macRDD.filter(x => top.contains(x._1)).sortBy(x => (x._1, x._2), ascending = true)

    //topMacID.repartition(1).saveAsTextFile(args(2))
    sc.stop()
  }
}
