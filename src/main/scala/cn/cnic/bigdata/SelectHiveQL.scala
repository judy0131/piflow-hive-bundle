package cn.cnic.bigdata

import cn.cnic.bigdatalab.utils.PropertyUtil
import cn.piflow._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession


class SelectHiveQL(hiveQL:String, dataframeOut:String) extends Process {




    override def shadow(pec: ProcessExecutionContext) = {

      val spark = pec.get[SparkSession]()

      import spark.sql
      val studentDF = sql(this.hiveQL)
      studentDF.show()


      new Shadow {
        override def discard(pec: ProcessExecutionContext): Unit = {
          //tmpfile.delete();
        }

        override def perform(pec: ProcessExecutionContext): Unit = {
          val dataframeOutPath : String = PropertyUtil.getPropertyValue("dataframe_hdfs_path") + dataframeOut + ".parquet"
          //TODO: delete the path
          val path = new Path(dataframeOutPath)
          val hdfs = FileSystem.get(new java.net.URI(PropertyUtil.getPropertyValue("hdfsURI")),spark.sparkContext.hadoopConfiguration)
          if (hdfs.exists(path)){
            System.out.println("Deleting " + dataframeOutPath + "!!!!!!!!!!!!!!")
            hdfs.delete(path,true)
          }
          studentDF.write.parquet(dataframeOutPath)
          //spark.close();
        }

        override def commit(pec: ProcessExecutionContext): Unit = {
          //tmpfile.renameTo(new File("./out/wordcount"));
        }
      };

    }

    /**
      * Backup is used to perform undo()
      *
      * @param pec
      * @return
      */
    override def backup(pec: ProcessExecutionContext): Backup = ???
  }


