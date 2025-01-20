package joins

import org.apache.commons.math3.stat.StatUtils.sum
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object DSETMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("DSET").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._



		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================

    //Read File and load data into RDD
    val textFile = sc.textFile(args(0))
    val split = textFile.map(line => line.split(","))
    val twitterFollowerCount = split.filter(line => line(1).toInt % 100 == 0)
      .map(user => (user(1), 1) )


    //Convert RDD to dataframe
    val dataframe =sparkSession.createDataFrame(twitterFollowerCount)

    //Group by User Id (Column 1) and aggregate by count (Column 2)
    val followerCountAggregate = dataframe.groupBy("_1").sum("_2").withColumnRenamed("sum(_2)", "totalCount")

    //Save output to textfile on path specified
    followerCountAggregate.rdd.saveAsTextFile(args(1))

    logger.info(followerCountAggregate.explain())
    logger.info(twitterFollowerCount.toDebugString)

  }
}