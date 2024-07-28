package api

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer

object dihedralMaster {

  def getListOfFiles(dir: String, spark: SparkSession): Array[String] = {
    val sc = spark.sparkContext
    var fileList = new scala.collection.mutable.ArrayBuffer[String]()
    val config = new Configuration()
    val path = new Path(dir)
    val fd = FileSystem.get(config).listStatus(path)
    fd.foreach(x => {
      fileList += x.getPath.toString()
    })
    return fileList.toArray
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.getOrCreate()
    val hconf = spark.sparkContext.hadoopConfiguration
    hconf.setInt("dfs.replication", 1)
    import spark.implicits._
    val sc = spark.sparkContext
    val r = new Read2()
    val prm_file = args(0)
    val crd_file_dir = args(1)
    val out_dir = args(2)
    var crd_files = getListOfFiles(crd_file_dir, spark).sorted

    val di = new Dihedral()
    val arr = r.read_pointers(prm_file, spark)

    var finishtime: Double = 0
    for (i <- 0 to crd_files.length - 1) {
      val temp = r.readPrmDih(prm_file, arr(0), arr(1), crd_files(i).toString, out_dir, spark)

      val time = System.currentTimeMillis()
      
      var dihedralframe = scala.collection.mutable.ListBuffer[String]()
      var frame_no = i*10+1
      for(frame <- temp)
        {
          val x = di.dihedralAngle(frame)
          dihedralframe += "Frame"+frame_no+": " +x
          frame_no += 1
        }
      sc.parallelize(dihedralframe).coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/data/dihedral_angles_"+i)

      val fintime: Double = (System.currentTimeMillis() - time) / 1000
      finishtime = finishtime + fintime
      
    }

    var time = new Array[Double](1)
    time(0) = (finishtime)
    sc.parallelize(time).coalesce(1).saveAsTextFile("hdfs:///user/ppr.gp2/data/Dihedral_time")
  }
}

















































