package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object FollowerCountMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.FollowerCountMain <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Follower Count")
    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    
    val textFile = sc.textFile(args(0))

    val counts = textFile.flatMap(line => line.split("/n"))
      //Split each record into two user ids and select second id for next processing step
      .map(record => record.split(",")(1))
      //Send only those user ids divisible by 100 to the next step
      .filter(userId => userId.toInt % 100 == 0 )
      .map(user => (user.toInt, 1))
      .reduceByKey(_ + _)
      //Sort on follower count in ascending order
      .sortBy(_._2)

    logger.info("Start of DebugString :-")
    logger.info(counts.toDebugString)

    counts.saveAsTextFile(args(1))
  }
}