package cn.cnic.bigdata

import cn.piflow._
import org.apache.spark.sql.SparkSession
import org.junit.Test


class FlowTest {

  val hdfsURI = "hdfs://10.0.86.89:9000"
  val dataframeHdfsPath = hdfsURI + "/xjzhu/"

  @Test
  def testHive(): Unit = {

    val flow: Flow = new FlowImpl();

    flow.addProcess("SelectHiveQL", new SelectHiveQL("select * from sparktest.student", "student"));
    flow.addProcess("PutHiveStreaming", new PutHiveStreaming("student","sparktest","studenthivestreaming"));
    flow.addTrigger("PutHiveStreaming", new DependencyTrigger("SelectHiveQL"));


    val spark = SparkSession.builder()
      .master("spark://10.0.86.89:7077")
      .appName("piflow-hive-bundle")
      .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "2g")
      .config("spark.cores.max", "2")
      .config("spark.jars","/opt/project/piflow-hive-bundle/out/artifacts/piflow_hive_bundle/piflow-hive-bundle.jar")
      .enableHiveSupport()
      .getOrCreate()

    val exe = Runner.bind("localBackupDir", "/tmp/")
      .bind(classOf[SparkSession].getName, spark)
      .run(flow);

    exe.start("SelectHiveQL");
    Thread.sleep(30000);
    exe.stop();

  }

}
