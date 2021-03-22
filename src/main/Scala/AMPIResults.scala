import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

object AMPIResults {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("AMPIResults")
            .getOrCreate()
        val sc = spark.sparkContext
        val hadoopConf = sc.hadoopConfiguration
        val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

        // (1,2865,0.4,1.0,0.0,0.5)
        val results = sc.textFile(args(0) + "/*").map(line => {
            val fields = line.drop(1).dropRight(1).split(",")
            val argString = fields.drop(2).mkString(",")
            val flag = fields(0).toInt
            val num = fields(1).toFloat
            (argString, (flag, num))
        })

        val processRDD = results.groupByKey().mapValues(line => {
            val data = line.toMap
            val acc = data(1) / (data(0) + data(1))
            (acc * 100).formatted("%.3f%%")
        })

        val filePath = args(0) + "/merge"
        val path = new Path(filePath)
        if(hdfs.exists(path))
            hdfs.delete(path,true)
        processRDD.repartition(1).sortByKey().saveAsTextFile(filePath)
        sc.stop()
    }
}
