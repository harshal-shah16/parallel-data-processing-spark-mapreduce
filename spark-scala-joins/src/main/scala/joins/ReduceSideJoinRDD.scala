package joins

import org.apache.commons.math3.stat.StatUtils.sum
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object ReduceSideJoinRDDMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir> <max>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("RDD-ReduceSideJoin").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val max = args(2).toInt

    val triangleCount = sc.longAccumulator("Triangle Count")

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    
    val textFile = sc.textFile(args(0))



    val AtoB = textFile.map(line => line.split(","))
                  .filter(line => (line(0).toInt < max) && (line(1).toInt < max))
                  .map(user => (user(0), user(1)) )


    val BtoC = textFile.map(line => line.split(","))
      .filter(line => (line(0).toInt < max) && (line(1).toInt < max))
      .map(user => (user(1), user(0)) )


    //After Join, pathTwo RDD is of type (user1, (user2, user3))
    //Reverse this output to ((user2, user3), user1) to join with edges in the next step
    val pathTwo = AtoB.join(BtoC)
      .map(user => (user._2, user._1))


    //Convert edges into ((user1, user2), null) format
    //When path2 RDD is joined with this RDD, result key-value pairs are triangles
    //For example, a path2 RDD key-value pair might be ((6,4), 5) (which means edges (4,5) & (5,6) exist
    // If CtoA RDD has ((6,4,1), null) key-value pair
    //Join on key which is (6,3) indicates a triangle has been found
    val CtoA = AtoB.map {
      user => ((user._1, user._2), null)
    }

    val triangles = pathTwo.join(CtoA)

    logger.info("Start of DebugString :-")
    logger.info(triangles.toDebugString)

    //triangles.saveAsTextFile(args(1))

    logger.info("triangles : "  + triangles.count())
    //Add triangle count to global accumulator
    triangleCount.add(triangles.count())
    
  }
}