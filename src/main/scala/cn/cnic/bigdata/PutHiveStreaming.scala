package cn.cnic.bigdata

import cn.cnic.bigdatalab.utils.PropertyUtil
import cn.piflow._
import org.apache.spark.sql.SparkSession
class PutHiveStreaming(dataframeIn:String, database:String, table:String) extends Process{

  val dataframeInPath = PropertyUtil.getPropertyValue("dataframe_hdfs_path") + dataframeIn + ".parquet"

  override def shadow(pec: ProcessExecutionContext) = {
    val spark = pec.get[SparkSession]()
    val studentParquetFileDF = spark.read.parquet(this.dataframeInPath)
    studentParquetFileDF.show()
    studentParquetFileDF.createOrReplaceTempView(this.dataframeIn)

    new Shadow {
      override def discard(pec: ProcessExecutionContext): Unit = {
        //tmpfile.delete();
      }

      override def perform(pec: ProcessExecutionContext): Unit = {
        spark.sql("insert into " + database + "." + table +  " select * from " + dataframeIn)
        //spark.close();
      }

      override def commit(pec: ProcessExecutionContext): Unit = {
        //tmpfile.renameTo(new File("./out/wordcount"));
      }
    };
  }

  override def backup(pec: ProcessExecutionContext): Backup = ???

}
