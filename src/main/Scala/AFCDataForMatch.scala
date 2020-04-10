import GeneralFunctionSets.{dayOfMonth_long, transTimeToTimestamp}
import org.apache.spark.sql.SparkSession

object AFCDataForMatch {

  /**
   * 提取不匹配的部分AFC数据，用于后面和匹配的AFC数据结合用于和AP数据做匹配
   */

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("AFCDataForMatch")
      .getOrCreate()
    val sc = spark.sparkContext

    // 读取ap和afc映射关系
    // (251449740,ECA9FAE07B4F,26.857,43,0.6245814)
    val groundTruthData = sc.textFile(args(0) + "/liutao/UI/GroundTruth/IdMap/part-*").map(line => {
      val fields = line.split(",")
      val afcId = fields(0).drop(1)
      afcId
    })
    val groundTruthMap = sc.broadcast(groundTruthData.collect().toSet)

    // 读取AFC数据: (020798332,2019-06-24 10:06:50,碧海湾,2019-06-24 10:25:09,桥头)
    // 所有afc数据路径：/liutao/UI/SampledAFCData/part-*
    val AFCFile = sc.textFile(args(0) + "/liutao/UI/SampledAFCData/part-*").map(line => {
      val fields = line.split(',')
      val id = fields(0).drop(1)
      val ot = transTimeToTimestamp(fields(1))
      val day = dayOfMonth_long(ot)
      (id, (line, day))
    }).filter(x => !groundTruthMap.value.contains(x._1))


    // 仅保留出行天数大于10天的数据
    val AFCPartitions = AFCFile.groupByKey().map(line => {
      val data = line._2.toList.sorted
      val daySets = data.map(_._2).toSet
      (data, daySets)
    }).filter(_._2.size > 9)

    val result = AFCPartitions.flatMap(line => {
      for (v <- line._1) yield
        v._1
    })

    result.repartition(10).saveAsTextFile(args(0) + "/liutao/UI/GroundTruth/afcData-not")


    sc.stop()


  }
}
