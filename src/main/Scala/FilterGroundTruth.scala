import org.apache.spark.sql.SparkSession

/**
 * 筛选ground truth数据
 */
object FilterGroundTruth {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("FilterGroundTruth")
            .getOrCreate()
        val sc = spark.sparkContext

        // (688629436,(3412F9946DFA,21.045,25,0.8417843))
        val IdMap = sc.textFile(args(0) + "/zlt/UI-2021/MatchResult/*/part-*").map(line => {
            val fields = line.split(",")
            val afcId = fields(0).drop(1)
            val apId = fields(1).drop(1)
            val ratio = fields(2)
            val num = fields(3)
            val score = fields.last.dropRight(2).toFloat
            (afcId, apId, ratio, num, score)
        }).filter(_._5 > 0.85).cache()


        val APIdSet = sc.broadcast(IdMap.map(_._2).collect().toSet)
        val AFCIdSet = sc.broadcast(IdMap.map(_._1).collect().toSet)

        IdMap.repartition(1)
            .sortBy(_._5, ascending = false)
            .saveAsTextFile(args(0) + "/zlt/UI-2021/GroundTruth/IdMap")

        val SampledAPFile = sc.textFile(args(0) + "/zlt/UI-2021/SampledAPData/part*").map(line => {
            val fields = line.split(",")
            val id = fields(0).drop(1)
            (id, line)
        }).filter(x => APIdSet.value.contains(x._1)).map(_._2)

        SampledAPFile.repartition(1).saveAsTextFile(args(0) + "/zlt/UI-2021/GroundTruth/SampledAPData")

        val APFile = sc.textFile(args(0) + "/zlt/UI/NormalMacData/part-*").map(line => {
            val fields = line.split(",")
            val id = fields(0).drop(1)
            (id, line)
        }).filter(x => APIdSet.value.contains(x._1)).map(_._2)
        APFile.repartition(1).saveAsTextFile(args(0) + "/zlt/UI-2021/GroundTruth/APData")

        val AFCFile = sc.textFile(args(0) + "/Destination/subway-pair/part*").map(line => {
            val id = line.split(",")(0).drop(1)
            (id, line)
        }).filter(x => AFCIdSet.value.contains(x._1)).map(_._2)
        AFCFile.repartition(1).saveAsTextFile(args(0) + "/zlt/UI-2021/GroundTruth/AFCData")

        sc.stop()

    }
}
