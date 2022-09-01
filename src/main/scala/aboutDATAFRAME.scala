import Util._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{AnyDataType, DataType, IntegerType, StringType, StructField, StructType}
object aboutDATAFRAME {
  def main(args:Array[String]):Unit= {
    Util.spark.sparkContext.setLogLevel("ERROR")

    val types=false
    if(types) {
      /** 1) Dataframes can be created in 3 ways
       *  //1)to.DF(column_names...) method -- needs a
       *  // a)scala collection(Array is not a scala collection) and we need to
       *  // b)import spark implicts
       *  // we need/can directly provide headers/column_names */
      import Util.spark.implicits._
      val someCollection = List(("nishanth", 31), ("Teju", 30)).toDF("Name", "Age")
      someCollection.show(false)
      /** 2) createDataframe method -- a) needs a RDD and schema/headers
       *  // and we need a Row function from import org.apache.spark.sql.Row
       *  // to pass headers we need to pass a schema separately which can be done by using Struct
       *  //below shows converting a collection to RDD using sparkContext Parallelize method */
      val dataRDD = spark.sparkContext.parallelize(Seq(Row(1, "nishanth", 31), Row(2, "Teju", 30)))
      //below show creating schema using StructType
      //StructFiled(column_name,columnType,isnull) --isnull that means if null values are allowed or not
      val schema = new StructType()
        .add(StructField("Id", IntegerType, false))
        .add(StructField("Name", StringType, true))
        .add(StructField("Age", IntegerType, true))
      val createDataFrameDF = spark.createDataFrame(dataRDD, schema)
      createDataFrameDF.show(false)

      //3) uisng spark.read from a file
      val usingReadDF = spark.read.format("csv").option("header", true).load("/Users/new/IdeaProjects/Pikachu/src/main/resources/CSV/Sales_Records.csv").limit(10)
      //ADDITIONAL
      //4) from existing Dataframe
    }
    val (df1,df2,dupDF)=sampleDF()
  val narrow=false
    if(narrow){
      /** NARROW TRANSFORMATIONS : Narrow transformations are the result of map() and filter() functions
       * and these compute data that live on a single partition meaning there will not be any data movement/shuffle
       * between partitions to execute narrow transformations. */

      /**1)MAP - requires an DF and a function as input and provides a new RDD with same rows/length applying the given
      // function on each element of the BASE DF
       argument: i)DF, ii)any function which can be applied on each row of the DF
       output: Dataset[Any,..] Any,..--depends on function used
       */
      import Util.spark.implicits._
      val mapDF=df1.map(x=>(x.getString(0).toUpperCase(),x.getInt(1),x.getInt(2))) // if we do not use
      // getCorrespondingType we get Exception in thread "main" java.lang.ClassNotFoundException: scala.Any
      val map2DF=df1.map(x=>x.toString.toUpperCase())
      println("************************************************************")
      println("ORGINAL DATAFRAME 1")
      df1.show()
      println("************************************************************")
      println("MAP DATAFRAME: ")
      mapDF.show()
      println("MAP DATAFRAME WITHOUT GET CORRESPONDING COLUMN TYPES: ")
      map2DF.show()
      println("************************************************************")

      /** 2)FILTER - requires an DF and a boolean operation as input and provides a new RDD with same or
       * less rows/length validating the given operation on each element of the BASE DF
         argument: i)DF,ii)any boolean operation which can be applied on each row of the DF
         output: Dataset[Row]
       */
       val filterDF=df1.filter(x=>x.getInt(1)>300)
      println("FILTER DATAFRAME: ")
      filterDF.show()
      println("************************************************************")

      /** 3) mapPartition */
      /** mapPartion - same as map but instead of applying the function on each row it applies the function on partition
       * requires:RDD,function, map and Iterator  as input
       * An iterator is a way to access elements of a collection one-by-one.
       * It resembles to a collection in terms of syntax but works differently in terms of functionality.
       * An iterator defined for any collection does not load the entire collection into the memory but
       * loads elements one after the other. Therefore, iterators are useful when the data is too large for the memory.
       * To access elements we can make use of hasNext() to check if there are elements available and next()
       * to print the next element.
        argument: i)DF, ii)function, iii)map and iv)Iterator
        output: Dataset[Any]
       */
      val mapPartitionDF = df1.mapPartitions(row => {
        val res = row.map(x => x.toString.toUpperCase)
        res
      })
      println("MAPPARTITIONS DATAFRAME: ")
      mapPartitionDF.show()
      println("************************************************************")

      /** 4) FLATMAP */
      /** With the help of flatMap() function, to each input element, we have many elements in an output RDD.
       * The most simple use of flatMap() is to split each input string into words.
       * Map and flatMap are similar in the way that they take a line from input RDD and apply a function on that line.
       * The key difference between map() and flatMap() is map() returns only one element, while flatMap() can return
       * a list of elements.
       argument: i)DF, ii)some pattern to split/faltten iii)split/flatten method
       output: Dataset[Any]
       */
      val flatMapDF = df1.flatMap(x => x.toString.replace("[","")
        .replace("]","").split(","))
      println("FLAT MAP DATAFRAME: ")
      flatMapDF.show()
      println("************************************************************")

      /** 5) UNION */
      /** UNION - Return a new dataset that contains the union of the elements in the source dataset and the argument.
       conditions: both source and target should have same columns and also same names
       argument: i)DF1 ii) DF2
       output: Dataset[Row]
       * */
      val unionDF = df1.select("book_name","writer_id").union(df2) //unionAll and union produces same result
      println("UNION DATAFRAME: ") //unionAll() method is deprecated since Spark “2.0.0” version and recommends using
      // the union() method.
      unionDF.show()
      println("************************************************************")

      /** 6) unionByName */
      /** in the latest versions of spark 3+ there is an option called unionByName
       * to merge/union two DataFrames with a different number of columns (different schema)
       * The difference between unionByName() function and union() is that this function
       * resolves columns by name (not by position).
       * In other words, unionByName() is used to merge two DataFrame’s by column names instead of by position. */
      /** val unionByNameRDD=rdd1.unionByName(rrd2,allowMissingCols=true) : currenly cannot implement because the pom
       * has 2.1 version  other ways to overcome for lower versions is by creating the missing columns with NULL
       * values for both RDDs and then perform union or unionAll */
//      val unionByNameDF=df1.unionByName(df2,allowMissingColumns=true)
//      println("UNIONBYNAME DATAFRAME: ")
//      unionByNameDF.show()
      println("unionByName available only from spark 3+")
      println("************************************************************")
      /** ----------------------------------NARROW TRANSFORMATIONS---------------------------------- */
    } // END OF NARROW TRANSFORMATIONS
val wide=true
    if(wide){
      /** ----------------------------------WIDE TRANSFORMATIONS---------------------------------- */
      /** WIDE TRANSFORMATIONS :  Wider transformations are the result of groupByKey() and reduceByKey() functions
       * and these compute data that live on many partitions meaning there will be data movements between partitions
       * to execute wider transformations. Since these shuffles the data, they also called shuffle transformations.
       * Functions such as groupByKey(), aggregateByKey(), aggregate(), join(), repartition() are some examples of a
       * wider transformations. */
      println("BEFORE APPYING DISTINCT DATAFRAME: ")
      dupDF.show()
      /** 1) DISTINCT */
      val distinctDF=dupDF.distinct()
      println("AFTER APPYING DISTINCT DATAFRAME: ")
      distinctDF.show()
      println("************************************************************")
      /** 2) DROPDUPLICATES */
      /** Duplicate rows could be remove or drop from Spark SQL DataFrame using distinct() and dropDuplicates() functions,
       * distinct() can be used to remove rows that have the same values on all columns whereas dropDuplicates()
       * can be used to remove rows that have the same values on multiple selected columns.
       * Spark doesn’t have a distinct method that takes columns that should run distinct on however,
       * Spark provides another signature of dropDuplicates() function which takes multiple columns to
       * eliminate duplicates. */
      val dropDuplicatestDF = dupDF.dropDuplicates("book_name","cost")
      println("AFTER APPYING dropDuplicates DATAFRAME: ")
      dropDuplicatestDF.show()
      println("************************************************************")

      /** 3) INTERSECT
       * Intersect can only be performed on tables with the same number of columns and same names
       */
      println("INTERSECT DF: ") //selects only common records between source and target
      val intersectDF = df1.intersect(dupDF)
      intersectDF.show()
      println("************************************************************")
      /** 4) reduceByKey */

      //adding a new column using map to provide key,value pair for reduceByKey
      val mapRDD2 = withoutEmptyRecordsRDD.map(m => (m, 1)) //MAP
      println("******************************************************************")
      println("MAP RDD after adding a new column with 1 to generate (K,V) Pair")
      mapRDD2.foreach(println)
      println("******************************************************************")
      /** reduceByKey – reduceByKey() merges the values for each key with the function specified. The reduce is carried
       * in such a way that level of reduction will be on each individual partition.
       * When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values
       * for each key are aggregated using the given reduce function func, which must be of type (V,V) => V.
       * Like in groupByKey, the number of reduce tasks is configurable through an optional second argument. */
      import Util.spark.implicits._
      val flatMapDF = dupDF.flatMap(x => x.toString.replace("[", "")
        .replace("]", "").split(","))
      val mapKeyDF = flatMapDF.map(m => (m,1)) //MAP
      val reduceDF = mapKeyDF.reduceByKey(_+_)
      println("******************************************************************")
      println("REDUCE RDD USING reduceByKey with + Operator")
      reduceDF.show()
      println("******************************************************************")
      val reduceDF2 = mapKeyDF.reduceByKey(_ - _)
      println("REDUCE RDD USING reduceByKey with - Operator")
      reduceDF2.show()
      println("***************
    }

  }
  //SAMPLE DF
  case class Book(book_name: String, cost: Int, writer_id: Int)
  case class Writer(writer_name: String, writer_id: Int)
  def sampleDF():(DataFrame,DataFrame,DataFrame) ={
    import Util.spark.implicits._
    val bookDF = Util.spark.sparkContext.parallelize(Seq(
      Book("Scala", 400, 1),
      Book("Spark", 500, 2),
      Book("Kafka", 300, 3),
      Book("Java", 350, 5)
    )).toDF
    val writerDF = Util.spark.sparkContext.parallelize(Seq(
      Writer("Martin", 1),
      Writer("Zaharia ", 2),
      Writer("Neha", 3),
      Writer("James", 4)
    )).toDF
    val bookDupDF = Util.spark.sparkContext.parallelize(Seq(
      Book("Scala", 400, 1),
      Book("Spark", 500, 2),
      Book("Kafka", 300, 3),
      Book("Scala", 400, 1)
    )).toDF
    return (bookDF,writerDF,bookDupDF)
  }

  def nestedStruct(): Unit ={

    /** nested struct type */
    val structureData = Seq(
      Row(Row("James ", "", "Smith"), "36636", "M", 3100),
      Row(Row("Michael ", "Rose", ""), "40288", "M", 4300),
      Row(Row("Robert ", "", "Williams"), "42114", "M", 1400),
      Row(Row("Maria ", "Anne", "Jones"), "39192", "F", 5500),
      Row(Row("Jen", "Mary", "Brown"), "", "F", -1)
    )
    val structureSchema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType)) //here the name struct ends
      .add("id", StringType)
      .add("gender", StringType)
      .add("salary", IntegerType)

    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData), structureSchema)

    //To break nested loop or to read all sub-columns with normal columns used .* after nested column name
    val nestedBreakDF = df2.select("name.*", "id", "gender", "salary")
    nestedBreakDF.show(false)
    // To apply/check if a value exists in a column/columns of a Dataframe
    nestedBreakDF.filter(nestedBreakDF("lastname") === "Jones" || nestedBreakDF("id") === "36636").show(false)

    //To add a new column to existing DF ..this creates a new DF
    // using lit because ..providing only string or int will throw error because we need to supply values to all rows not single row
    nestedBreakDF.withColumn("newColumn", lit("1")).show()
    //to view column name as someother name
    nestedBreakDF.withColumnRenamed("middlename", "midname").show()

    //any dataframe can be used to create table or view
    nestedBreakDF.createOrReplaceTempView("SomeTable")
    //and then we can perform any sql queries on this table/view using spark.sql
    val queryResDF = spark.sql("select * from SomeTable limit 2")
  }

}

/**
//REF
//StructType – Defines the structure of the Dataframe
// Spark provides spark.sql.types.StructType class to define the structure of the DataFrame and
// It is a collection or list on StructField objects.
//Using StructField we can also add nested struct schema, ArrayType for arrays and MapType for key-value pairs

//spark.read.text() method is used to read a text file into DataFrame.
// like in RDD, we can also use this method to read multiple files at a time,
// reading patterns matching files and finally reading all files from a directory.

//refer this link below for more deatils
https://sparkbyexamples.com/spark/spark-read-text-file-rdd-dataframe/


*/