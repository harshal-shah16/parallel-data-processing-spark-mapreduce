package joins

import org.apache.commons.math3.stat.StatUtils.sum
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object ReduceSideJoinDatasetMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir> <max>")
      System.exit(1)
    }


    val conf = new SparkConf().setAppName("ReduceSideJoin-Dataset").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

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
      .map(user => (user(0), user(1)) )


    //Convert RDD to dataframe
    var AtoBDataFrame = sparkSession.createDataFrame(AtoB).toDF("AtoB_1", "AtoB_2")
    val BtoCDataFrame = sparkSession.createDataFrame(BtoC).toDF("BtoC_1", "BtoC_2")
    //val CtoADataFrame = sparkSession.createDataFrame(CtoA).toDF("CtoA_1", "CtoA_2")

    //Create Path2 by joining AtoB with BtoC
    val path2 = AtoBDataFrame.join(BtoCDataFrame).where(AtoBDataFrame("AtoB_2") === BtoCDataFrame("BtoC_1") && AtoBDataFrame("AtoB_1") =!= BtoCDataFrame("BtoC_2"))

    //path2.show()

    //Rename column names of AtoB dataframe
    AtoBDataFrame = AtoBDataFrame.withColumnRenamed("AtoB_1", "start").withColumnRenamed("AtoB_2", "end")
    //AtoBDataFrame.show()


    //Join path2 with edges to form triangles
    //Check for Path2 start node with edges second node and Path2 end node with edge first node
    val triangles = path2.join(AtoBDataFrame).where(path2("BtoC_2") === AtoBDataFrame("start") && path2("AtoB_1") === AtoBDataFrame("end"))

    logger.info("Triangles Count : " + triangles.count());
    triangleCount.add(triangles.count())
  }
}