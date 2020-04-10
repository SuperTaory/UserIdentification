import org.apache.spark.sql.SparkSession
import GeneralFunctionSets.{dayOfMonth_long, transTimeToTimestamp}

object FilterGroundTruth {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Matching Model")
      .getOrCreate()
    val sc = spark.sparkContext

    val partAfc = sc.textFile(args(0) + "/Destin/subway-seq/part-000[0-2]*").map(line => {
      val fields = line.split(',')
      val pid = fields(0).drop(1)
      (pid, 1)
    }).groupByKey().mapValues(_.size/2)

    val map1 = sc.broadcast(partAfc.collect().toMap)


    val matchResult = sc.textFile(args(0) + "/liutao/UI/MatchPerMonth/p000*").map(line => {
      val fields = line.split(",")
      val afcId = fields(0).drop(1)
      val apId = fields(1)
      var score = 0f
      var num = 0
      if (fields.length == 3) {
        score = fields(2).dropRight(1).toFloat
        num = map1.value(afcId)
      } else if (fields.length == 4) {
        score = fields(2).toFloat
        num = fields(3).dropRight(1).toInt
      }
      (afcId, (apId, score, num))
    }).groupByKey().mapValues(x => x.toList.maxBy(_._2))

    val groundTruth = matchResult.map(x => {
      (x._1, x._2._1, x._2._2, x._2._3, x._2._2 / x._2._3)
    }).filter(_._5 > 0.6).cache()

    val selectAPId = sc.broadcast(groundTruth.map(_._2).collect().toSet)
    val selectAFCID = sc.broadcast(groundTruth.map(_._1).collect().toSet)

    groundTruth.repartition(1).sortBy(_._5, ascending = false).saveAsTextFile(args(0) + "/liutao/UI/GroundTruth/IdMap")

    val APFile = sc.textFile(args(0) + "/liutao/UI/SampledAPData-new/part*").map(line => {
      val fields = line.split(",")
      val id = fields(0).drop(1)
      (id, line)
    }).filter(x => selectAPId.value.contains(x._1)).map(_._2)

    APFile.repartition(1).saveAsTextFile(args(0) + "/liutao/UI/GroundTruth/apData")

    val AFCFile = sc.textFile(args(0) + "/Destin/subway-pair/part*").map(line => {
      val id = line.split(",")(0).drop(1)
      (id, line)
    }).filter(x => selectAFCID.value.contains(x._1)).map(_._2)
    AFCFile.repartition(1).saveAsTextFile(args(0) + "/liutao/UI/GroundTruth/afcData")

    sc.stop()

  }
}
