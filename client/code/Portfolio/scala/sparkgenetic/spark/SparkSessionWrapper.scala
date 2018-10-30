package sparkgenetic.spark

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.xml.XML

trait SparkSessionWrapper{
	System.setProperty("HADOOP_USER_NAME", "cloudera")
	val cores = "4"
	lazy val spark = SparkSession.builder()
  	.appName("SparkGenetic") 
	 	//.config("deploy-mode", "cluster")
  	.master("local["+cores+"]")
		.config("spark.driver.cores", cores)
		.config("spark.driver.memory", "10g")
		.config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:/home/osiris_wrath/Code/Optimization/client/code/Portfolio/src/main/scala/resources/log4j.properties" +"-XX:+UseG1GC"/*-XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThread=8"*/)
		//Aprox. 3 times the number of driver cores
		//MEMORY AND RDD PARTITIONING
		.config("spark.sql.shuffle.partitions", /*"3"*/"4") //default 200, ok with 7, great with 3 per core - DOESNT SEEM TO BE IN EFFECT. NEED REPARTITION
		//until 128mb for broadcast
		.config("spark.sql.autoBroadcastJoinThreshold", "134217728")
		//amount of shared memory space for executuion and storage : M
		//.config("spark.memory.fraction", "0.8")
		//amount of non eviction storage memory - our RDD takes 350mb ~ - subset of M called R
		//.config("spark.memory.storageFraction", "0.3")
		//.config("spark.eventLog.enabled", "true")    
		//.config("spark.eventLog.dir","hdfs:///user/spark/applicationHistory") 
		//.config("spark.executor.instances", "64")
		//Not used in Local mode -> not the same as standalone
		.config("spark.executor.memory", "10g")
		.config("spark.executor.cores", "4")
		.config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:/home/osiris_wrath/Code/Optimization/client/code/Portfolio/src/main/scala/resources/log4j.properties"/*+"-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThread=8"*/)
	  //MASTER AND YARN CONFS
		//.master("yarn")
	  //.config("spark.hadoop.fs.defaultFS", "hdfs://192.168.1.244:8020")
	  //.config("spark.hadoop.yarn.resourcemanager.address","192.168.1.244:8032")
	  //.config("spark.yarn.jars", "hdfs://quickstart.cloudera:8020/user/cloudera/jars/*.jar")
	  .config("spark.yarn.jars", "file:///home/osiris_wrath/Code/jars/*.jar")
	  //.config("spark.yarn.application.classpath", "$HADOOP_CLIENT_CONF_DIR,$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*")
	  .getOrCreate
}