import org.apache.spark.rdd.RDD

import scala.reflect.io.File

object aboutRDD {
/*Spark RDD can be created in several ways using Scala & Pyspark languages, for example,
It can be created by using sparkContext.parallelize(), from text file, from another RDD, DataFrame, and Dataset.*/

  ///////////creatingRDDUsingParallelize//////////////////
  //you will get a resulting RDD with only one element = content
  def creatingRDDUsingParallelize(anyArray: Array[Any]): RDD[Any] ={
    val someRDD=Util.spark.sparkContext.parallelize(anyArray)
    someRDD
  }

  def creatingRDDUsingParallelize(anySeq: Seq[Any]): RDD[Any] ={
    val someRDD=Util.spark.sparkContext.parallelize(anySeq)
    someRDD
  }

  def creatingRDDUsingParallelize(anyList: List[Any]): RDD[Any] ={
    val someRDD=Util.spark.sparkContext.parallelize(anyList)
    someRDD
  }
  ///////////creatingRDDUsingParallelize//////////////////

  ///////////creatingRDDUsingAnyFile//////////////////

  //you will get a resulting RDD with first element = filepath and second element = content
def creatingRDDUsingTextFile(pathToLoad:String): RDD[String] ={
  val someRDD=Util.spark.sparkContext.textFile(s"$pathToLoad")
  someRDD
}

  def creatingRDDUsingTextFiles(pathToLoad:String): RDD[(String,String)] ={
    val someRDD=Util.spark.sparkContext.wholeTextFiles(s"$pathToLoad")
    someRDD
  }

  ///////////creatingRDDUsingAnyFile//////////////////

def removefirstRow(data:RDD[(String,String)]):RDD[(String,String)]={
  val header = data.first()
  val data2= data.filter(row => row != header)
  data2
}

  def splitRDD(data:RDD[(String,String)]):RDD[String]={
    val data2= data.flatMap( e=>e._2.split("[?;\\n]+")).filter(_.nonEmpty)

    data2
  }

}
