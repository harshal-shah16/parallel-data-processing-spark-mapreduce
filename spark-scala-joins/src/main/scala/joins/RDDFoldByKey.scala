package joins

import org.apache.commons.math3.stat.StatUtils.sum
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RDDFoldByKeyMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RDD-FoldByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    
    val textFile = sc.textFile(args(0))
    val split = textFile.map(line => line.split(","))

    val twitterFollowerCount =  split.filter(line => line(1).toInt % 100 == 0)
      .map(user => (user(1), 1) )
      .foldByKey(0)(_ + _)


    twitterFollowerCount.saveAsTextFile(args(1))

    logger.info(twitterFollowerCount.toDebugString)


  }
}