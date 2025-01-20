package joins

import org.apache.commons.math3.stat.StatUtils.sum
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

import scala.collection.convert.ImplicitConversions.`map AsJavaMap`
import scala.collection.mutable

object ReplicatedJoinRDDMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir> <max>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("ReplicatedJoin-RDD")
    val sc = new SparkContext(conf)
    val max = args(2).toInt

    val triangleCount = sc.longAccumulator

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    //val hashMap = new mutable.HashMap[String, mutable.HashSet[String]]
    val textFile = sc.textFile(args(0))


    val AtoB = textFile.map(line => line.split(","))
      .filter(line => (line(0).toInt < max) && (line(1).toInt < max))
      .map(user => (user(0), user(1)) )

    //AtoB.map({
      //                user =>
        //                val usersFollowed = hashMap.getOrDefault(user._1, new mutable.HashSet[String]())
          //              usersFollowed.add(user._2)
            //            //hashMap.put(user._1, usersFollowed)
    //})


    //Create a pair RDD with value as a Set of all userIds that the Key userId follows
    //_++_ indicates List concatenation
    val aggregatedAtoB = AtoB.map(user => (user._1, Set(user._2)))
      .reduceByKey(_++_)

    //Broadcast after converting RDD to Map
    val broadcastMap = sc.broadcast(aggregatedAtoB.collect.toMap)

    //Check conditions for triangle. If it fulfills increment global counter
    val triangles = AtoB.collect.map ({ user =>
        broadcastMap.value.getOrElse(user._2, Set[String]()).foreach {
          userThree => if(broadcastMap.value.getOrElse(userThree, Set[String]()).contains(user._1)) {
            triangleCount.add(1)
          }
        }
    })


    //triangles.collect

    //triangles.saveAsTextFile(args(1))

    logger.info("Triangles : "  + triangleCount.value)

    //triangleCount.add(triangles.count)
    
  }
}