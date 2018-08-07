

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "cloudera")
    val spark = SparkSession.builder
        .appName("SparkTest") 
        .master("yarn")
	    	.config("deploy-mode", "cluster")
	      //.config("spark.hadoop.fs.defaultFS", "hdfs://192.168.1.244:8020")
	      //.config("spark.hadoop.yarn.resourcemanager.address","192.168.1.244:8032")
	      //.config("spark.hadoop.yarn.resourcemanager.hostname", "192.168.1.244")
	      .config("spark.yarn.jars", "hdfs://192.168.1.244:8020/user/cloudera/jars/*.jar")
	      //.config("spark.yarn.application.classpath", "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*.$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*")
	      .getOrCreate;
	    
	    val df = spark.read
	        .format("csv")
	        .option("header", "true") //reading the headers
	        .option("mode", "DROPMALFORMED")
	        .option("delimiter",";")
	        .load("hdfs://192.168.1.244:8020/user/cloudera/data/*.csv")
	    
	    df.sort(asc("Date"), asc("Symbol")).show()
	        
	    println(df.show())
  }
}