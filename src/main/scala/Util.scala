import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Util {

  // Upto spark version 1.6 we only have sparkContext which is the entry point of the spark Application
  // other contexts lke SqlContext,..are built on top of SparkContext
  // val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
  //After spark 1.6 we have SparkSession which contains sparkContext,SqlContext....
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("SparkByExamples.com")
    .getOrCreate()


//  val sc = spark.sparkContext
//  spark.conf.set("eventLog.enabled",true)
//  spark.conf.set("history.ui.port",4040)
//  spark.conf.set("eventLog.dir","/Users/new/sparkLogs")

  //  val sc = new spark.sparkContext()
  //  val spark=new SparkSession(conf)
  //SETTING LOG LEVEL CAN BE DONE TO SC OR LOGGER OBJECT BUT SECOND ONE IS PREFERABLE
  //sc.setLogLevel("ERROR")

  //to set log level
  def LogLevel(logType:Level): Unit ={
    Logger.getLogger("org").setLevel(logType)
  }

  //print all elements of RDD (Any type)
  def printRDD(someRDD:RDD[Any]): Unit ={
    someRDD.foreach(println)
  }

  //get datatype in a detailed manner
  def getClass(someValue: Any){
    println(s"The type of "+ someValue +" is :"+ someValue.getClass)
  }

}
