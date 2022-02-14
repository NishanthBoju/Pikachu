import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object subject1 {

  def main(args: Array[String]): Unit = {

   val spark=Util.spark
   Util.LogLevel(Level.OFF)
   val someRDD=aboutRDD.creatingRDDUsingParallelize(Array(1,2,3))
   //aboutRDD.printRDD(someRDD)
   val textFileRDD=aboutRDD.creatingRDDUsingTextFile("C:\\Users\\15148\\IdeaProjects\\Pikachu\\src\\main\\scala\\sampleTextFile.txt")
   val wholeTextFileRDD=aboutRDD.creatingRDDUsingTextFiles("C:\\Users\\15148\\IdeaProjects\\Pikachu\\src\\main\\scala\\*.txt")
   val splitWholeRDDRDD= aboutRDD.splitRDD(wholeTextFileRDD)
   //splitWholeRDDRDD.foreach(println)
   println(splitWholeRDDRDD.count())
   if(false){
   val x = Seq(1, 2, 3)
   val xRDD = spark.sparkContext.parallelize(x) //creates an RDD of type class org.apache.spark.rdd.ParallelCollectionRDD
   xRDD.collect().foreach(println)
   println("The class: " + xRDD.getClass)
   println("take :") // prints first n elements of RDD.
   xRDD.take(2).foreach(println) //  in case we enter 0 or -n .It doesn't throw any error but wont display anything
    import spark.implicits._
    val filename = new StructType().add($"filename".string)
    val content = new StructType().add($"content".string)
   }
   if(false) {
    //Creating an RDD on top a text file
    val textRDD = spark.sparkContext.wholeTextFiles("C:\\Users\\15148\\IdeaProjects\\Pikachu\\src\\main\\scala\\*.txt")
    //splitting by using space as identifier and making columns depending on number of words
    import spark.implicits._
    val textRDDDF = textRDD.toDF()
    println("Printing from Dataframe built on top of RDD: ")
    //println(textRDDDF.show(false))
    textRDDDF.printSchema()
    //creating a Dataframe with Headers on top of RDD
    val withheaderstextrddDF = spark.createDataFrame(textRDD).toDF("filename", "content")
    //withheaderstextrddDF.show(false)
    withheaderstextrddDF.printSchema()
    // withheaderstextrddDF.select("content").show(false)
    println(withheaderstextrddDF.count())
    val contentValue = withheaderstextrddDF.select("content").first
    val contentString: String = contentValue.toString()
    //  print(contentString)
    //to split a string based on newline and without empty values
    val splitDF: Array[String] = contentString.split("\n").filter(_.nonEmpty)
    // val nonEmptyArray=splitDF.filter(_.nonEmpty)
    println("splitDF details: ")
    println(splitDF.getClass)
    splitDF.foreach(println)
    println(splitDF.size)
    //val contentString = withheaderstextrddDF.select("content").filter(f=> f != header)
    //println(contentString.show(false))
    /*Spark map() is a transformation operation that is used to apply the transformation on every element of
   RDD, DataFrame, and Dataset and finally returns a new RDD/Dataset respectively.*/
    //val modifyDF=newDF.map()
    //printing the number of partitions of an RDD..
    // the number of partitions depends on number of files or blocks inside the directory provided
    println("Number of partitions of textRDD: " + textRDD.getNumPartitions)
    println("textRDD type: " + textRDD.getClass)
    //print number of words in a RDD
    println("Number of words: " + textRDD.count())
   }
    println("END")
    println("Stopping the spark Session now")
    spark.stop()
  }
}
