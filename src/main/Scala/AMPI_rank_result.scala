import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.Path

object AMPI_rank_result {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("AMPI_rank_result")
            .getOrCreate()
        val sc = spark.sparkContext
        val hadoopConf = sc.hadoopConfiguration
        val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

        // 读取groundTruth计算Accuracy (251449740,ECA9FAE07B4F,26.857,43,0.6245814)
        val groundTruthData = sc.textFile(args(0) + "/zlt/UI-2021/GroundTruth/IdMap/part-*").map(line => {
            val fields = line.split(",")
            val afcId = fields(0).drop(1)
            val apId = fields(1)
            (apId, afcId)
        })
        val groundTruthMap = sc.broadcast(groundTruthData.collect().toMap)

        val file = sc.textFile(args(0) + "zlt/UI-2021/SIG_rank/" + args(1) + "%").map(line => {
            val fields = line.drop(1).dropRight(1).split(",")
            val ap_id = fields(0)
            val afc_id_list = fields(1).split("-").reverse
            (ap_id, afc_id_list)
        })

        val resultMap = file.map(line => {
            val rank = line._2.take(args(2).toInt)
            var flag = 0
            if (rank.contains(groundTruthMap.value(line._1))) {
                flag = 1
            }
            (flag, 1)
        }).reduceByKey(_ + _).repartition(1).map(x => (x._1, x._2, args(1) + "%", args(2)))

        val filePath = args(0) + "zlt/UI-2021/SIG_rank_res/" + args.takeRight(2).mkString("_")
        val path = new Path(filePath)
        if(hdfs.exists(path))
            hdfs.delete(path,true)
        resultMap.saveAsTextFile(filePath)
        sc.stop()
    }

}
