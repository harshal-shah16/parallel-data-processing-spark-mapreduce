package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.intToLiteral

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object PageRankMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nwc.PageRankMain k iterations output")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Spark-PageRank").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val k = args(0).toInt

    //Create a mutable List for edges and page rank
    var edges = new ListBuffer[(Int, Int)]()
    var pagerank = new ListBuffer[(Int, Double)]()

    var totalDanglingPRMass: Double = 0.0

    //set the initial PR value of the dummy page to 0
    pagerank+=((0, 0.0))
    //Create graph of k*k edges
    //If k is divisible by 3 -> dangling page connect to O
    //else chain to the next vertex
    for( i <- 1 to k*k) {
      if (i % k != 0) {
        edges+=((i, i+1))
      } else {
        edges+=((i, 0))
      }
      pagerank+=((i, 1.0/(k*k) ))
    }

    //println(edges)
    //println(pagerank)


    //Convert graph and page rank into RDDs
    val graph = sc.parallelize(edges).map(page => (page._1, List(page._2)))
      .reduceByKey(_++_).cache()
    var pageRankRDD = sc.parallelize(pagerank).partitionBy(graph.partitioner.get)


    val iterations = args(1).toInt
    for(_ <- 1 to iterations) {


      //Join graph RDD with pagerank RDD
      //Map on values of this join to get a RDD such that key is a node and value being its incoming PR contributions
      var contribs = graph.join(pageRankRDD)
        .flatMap {
          case (page, (toPages, pagerank)) =>
            toPages.flatMap(toPage => List((toPage, pagerank), (page, 0.0)))
        }
        .reduceByKey(_ + _)


      //Capture PageRank value of dummy node zero
      totalDanglingPRMass = contribs.lookup(0).head

      //Make page rank of dummy node zero and distribute its PR to all nodes
      pageRankRDD = contribs.map {
        case (page, rank) => if (page == 0) {
          (page, 0.0)
        } else {
          (page, 0.15 * (1.0 / (k * k)) + 0.85 * (rank + totalDanglingPRMass * (1.0 / (k * k))))
        }
      }.sortByKey()
    }



   logger.info(pageRankRDD.toDebugString)

    pageRankRDD.saveAsTextFile(args(2))

    logger.info("Dummy Node 0 PR at the end of last iteration : " + totalDanglingPRMass)


  }
}