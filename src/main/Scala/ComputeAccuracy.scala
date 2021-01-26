import org.apache.spark.sql.SparkSession

object ComputeAccuracy {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("Matching Model")
            .getOrCreate()
        val sc = spark.sparkContext

        // 读取GroundTruth (668367478,ECD09FC6C6C5,24.0,24,1.0)
        val groundTruthData = sc.textFile(args(0)).map(line => {

        })

    }
}
