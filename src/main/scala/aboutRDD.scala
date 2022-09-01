import Util.{spark}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, functions}
import org.apache.spark.sql.functions.{input_file_name, length, lit}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.io.StdIn


object aboutRDD {

  def main(args: Array[String]):Unit={

    Util.spark.sparkContext.setLogLevel("ERROR")
    //val parquetDF=Util.spark.read.format("parquet").load
    ("/Users/new/IdeaProjects/Pikachu/src/main/resources/PARQUET/userdata5.parquet")
   // parquetDF.show(2,false)
   // aboutIterator() -- to find how itertor works run this
   val (rdd1, rdd2, new_rdd1, new_rdd2, df1, df2) = rddForUnionByName()
    val colPath="/Users/new/IdeaProjects/Pikachu/src/main/resources/CSV/Book1.csv".split("/")
   val csvDF=Util.spark.read.format("csv").
     option("header",true).load("/Users/new/IdeaProjects/Pikachu/src/main/resources/CSV/Book1.csv")
     .withColumn("filename",input_file_name).withColumn("subfile",lit(colPath(colPath.length-1)
     .substring(1)))
   //csvDF.show(false)
    val narrowtransformations=false // to do not run transformations ..change to false
    if(narrowtransformations) {
      val someRDD = someRDDMeth()
      someRDD.cache()
      println(someRDD.take(5).last)
      println("START BASE RDD")
      someRDD.foreach(println)
      println("END BASE RDD")

      /** ----------------------------------NARROW TRANSFORMATIONS---------------------------------- */

      /** NARROW TRANSFORMATIONS : Narrow transformations are the result of map() and filter() functions
       * and these compute data that live on a single partition meaning there will not be any data movement/shuffle
       * between partitions to execute narrow transformations. */
      //MAP - requires an RDD and a function as input and provides a new RDD with same rows/length applying the given
      // function on each element of the BASE RDD
      val t1 = System.nanoTime
      /** 1) MAP */
      val mapRDD = someRDD.map(x => x.toString().toUpperCase())
      println("\nSTART MAP RDD")
      mapRDD.foreach(println)
      val t2 = (System.nanoTime - t1) / 1e9d
      println("Duration for map :" + t2)
      println("END MAP RDD")
      //FILTER - requires an RDD and a BOOLEAN OPERATION and provides a new RDD with same/less rows validating
      // the given boolean operation on each element of the BASE RDD
      val t11 = System.nanoTime
      /** 2) FILTER */
      val filterRDD = someRDD.filter(x => x.get(0).toString.equalsIgnoreCase("Michael "))
      println("\nSTART FILTER RDD")
      filterRDD.foreach(println)
      val t3 = (System.nanoTime - t11) / 1e9d
      println("Duration for filter :" + t3)
      println("END FILTER RDD")

      /** 3) mapPartition */
      /** mapPartion - same as map but instead of applying the function on each row it applies the function on partition
       *  requires a RDD,function, map and Iterator  as input
       *  An iterator is a way to access elements of a collection one-by-one.
       * It resembles to a collection in terms of syntax but works differently in terms of functionality.
       * An iterator defined for any collection does not load the entire collection into the memory but
       * loads elements one after the other. Therefore, iterators are useful when the data is too large for the memory.
       * To access elements we can make use of hasNext() to check if there are elements available and next()
       * to print the next element. */
      val t111 = System.nanoTime
      println("\nSTART mapPartion RDD")
      val mapPartitionRDD = someRDD.mapPartitions(x => {
        val res = x.map(d => d.toString().toUpperCase())
        res
      })
      mapPartitionRDD.foreach(println)
      val t4 = (System.nanoTime - t111) / 1e9d
      println("Duration for mapPartion :" + t4)
      println("END mapPartion RDD")

      /** 4) FLATMAP */
      /** With the help of flatMap() function, to each input element, we have many elements in an output RDD.
       * The most simple use of flatMap() is to split each input string into words.
       * Map and flatMap are similar in the way that they take a line from input RDD and apply a function on that line.
       * The key difference between map() and flatMap() is map() returns only one element, while flatMap() can return
       * a list of elements. */
      //FLAT MAP
      val t1111 = System.nanoTime
      println("\nSTART flatmap RDD")
      val flatmapRDD = someRDD.flatMap(x => x.toString().trim().replace("[", "")
        .replace("]", "").split
      (",")).filter(x => x.nonEmpty)
      flatmapRDD.foreach(println)
      val t5 = (System.nanoTime - t1111) / 1e9d
      println("Duration for flatmapRDD :" + t5)
      println("END flatmap RDD")

      /** 5) UNION */
      /** UNION - Return a new dataset that contains the union of the elements in the source dataset and the argument.
       *  // conditions: both source and target should have same columns and also same names */
      /** val unionDF=parquetDF.union(csvDF) */
      //ERROR : Union can only be performed on tables
      // with the same number of columns, but the first table
      // has 13 columns and the second table has 10 columns;
      println("\nSTART unionRDD RDD")
      val csv1RDD = csvDF.rdd.take(5)
      val csv2RDD = csvDF.rdd.take(3)
      val unionRDD = csv1RDD.union(csv2RDD) // total 8 records and also has duplicates if
      // both source and target has any same records
      println("UNION RDD:")
      unionRDD.foreach(println)

      /** 6) unionByName */
      /** in the latest versions of spark 3+ there is an option called unionbyName
       *  to merge/union two DataFrames with a different number of columns (different schema)
       *  The difference between unionByName() function and union() is that this function
       * resolves columns by name (not by position).
       * In other words, unionByName() is used to merge two DataFrame’s by column names instead of by position. */
      /** val unionByNameRDD=rdd1.unionByName(rrd2,allowMissingCols=true) : currenly cannot implement because the pom
       * has 2.1 version  other ways to overcome for lower versions is by creating the missing columns with NULL
       * values for both RDDs and then perform union or unionAll */
      //In the above tuple the new_RDDs have the missing columns added with NULL values..
      //for more details of how to add missing columns in both RDDs refer method rddForUnionByName
      val unionMissingDF = new_rdd1.union(new_rdd2)
      println("UNION MISSING COLS RDD:")
      println("RDD1")
      rdd1.foreach(println)
      println("RDD2")
      rdd2.foreach(println)
      println("RDD1 with MISSING COLS ADDED WITH NULL")
      new_rdd1.foreach(println)
      println("RDD2 with MISSING COLS ADDED WITH NULL")
      new_rdd2.foreach(println)
      println("final union with null RDD")
      unionMissingDF.foreach(println)
      println("END unionRDD and unionByNAme")
    }
    //MORE about transformations  https://www.youtube.com/watch?v=7bVw7SJbXDY
      /** ----------------------------------NARROW TRANSFORMATIONS---------------------------------- */
      val widetransformations = false // to do not run transformations ..change to false
      if (widetransformations) {
        /** ----------------------------------WIDE TRANSFORMATIONS---------------------------------- */
        /** WIDE TRANSFORMATIONS :  Wider transformations are the result of groupByKey() and reduceByKey() functions
         * and these compute data that live on many partitions meaning there will be data movements between partitions
         * to execute wider transformations. Since these shuffles the data, they also called shuffle transformations.
         * Functions such as groupByKey(), aggregateByKey(), aggregate(), join(), repartition() are some examples of a
         * wider transformations. */

        /** 1) DISTINCT */
        val csv1RDD = csvDF.rdd.take(5)
        val csv2RDD = csvDF.rdd.take(3)
        val unionRDD = csv1RDD.union(csv2RDD)
        val unionDistinctRDD = unionRDD.distinct //to avoid duplicates use distinct
        println("UNION DISTINCT RDD:")
        unionDistinctRDD.foreach(println)

        /** 2) DROPDUPLICATES */
        /** Duplicate rows could be remove or drop from Spark SQL DataFrame using distinct() and dropDuplicates() functions,
         * distinct() can be used to remove rows that have the same values on all columns whereas dropDuplicates()
         * can be used to remove rows that have the same values on multiple selected columns.
         * Spark doesn’t have a distinct method that takes columns that should run distinct on however,
         * Spark provides another signature of dropDuplicates() function which takes multiple columns to
         * eliminate duplicates. */

        val dropDupDF = csvDF.dropDuplicates("TYPE", "READ") //applicable only for dataframe and dataset not RDD
        println("DROP DUPLICATES DF:")
        dropDupDF.show(false)

        /** 3) INTERSECT */
        println("INTERSECT RDD: ") //selects only common records between source and target
        val intersectRDD = csv1RDD.intersect(csv2RDD)
        intersectRDD.foreach(println)

        /** 4) reduceByKey */
        val textFileRDD = creatingRDDUsingTextFile("/Users/new/IdeaProjects/Pikachu/src/main/resources/TEXT/sample.txt")
        // val textFilesRDD = creatingRDDUsingTextFiles("/Users/new/IdeaProjects/Pikachu/src/main/resources/TEXT/*")
        //splitting the RDD using flatMap and using split by ,. and newline
        val flatMapRDD = textFileRDD.flatMap(x => x.split("[,.\n]")) //FLATMAP
        //to remove empty records on RDD use filter
        val withoutEmptyRecordsRDD = flatMapRDD.filter(row => !row.isEmpty) //FILTER
        //adding a new column using map to provide key,value pair for reduceByKey
        val mapRDD2 = withoutEmptyRecordsRDD.map(m => (m, 1)) //MAP
        println("******************************************************************")
        println("MAP RDD after adding a new column with 1 to generate (K,V) Pair")
        mapRDD2.foreach(println)
        println("******************************************************************")
        /** reduceByKey – reduceByKey() merges the values for each key with the function specified. The reduce is carried
         *  in such a way that level of reduction will be on each individual partition.
         *  When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values
         *  for each key are aggregated using the given reduce function func, which must be of type (V,V) => V.
         *  Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.
         conditions: ONLY applicable to PAIR RDDs (K,V)
         more about pair RDD Transformations:
        https://www.oreilly.com/library/view/learning-spark/9781449359034/ch04.html
         */
        val reduceRDD = mapRDD2.reduceByKey(_ + _)
        println("******************************************************************")
        println("REDUCE RDD USING reduceByKey with + Operator")
        reduceRDD.foreach(println)
        println("******************************************************************")
        val reduceRDD2 = mapRDD2.reduceByKey(_ - _)
        println("REDUCE RDD USING reduceByKey with - Operator")
        reduceRDD2.foreach(println)
        println("******************************************************************")

        /** 5) groupBy and groupByKey */
        /** groupBY is similar to reduceBykey but we combine and bring all the similar keys to one partition.and dont
         * need a addition operator.so too much shuffle not good to use for large data...which might result in going
         * out of memory/stack overflow. When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs.
         * Note: If you are grouping in order to perform an aggregation (such as a sum or average) over each key,
         *  using reduceByKey or aggregateByKey will yield much better performance.
         * Note: By default, the level of parallelism in the output depends on the number of partitions of the parent RDD.
         *  You can pass an optional numPartitions argument to set a different number of tasks. */
        println("******************************************************************")
        println("GROUPBY RDD after performing groupBy Transformation")
        val groupRDD = reduceRDD.groupBy(p => p._2) //_.2 indicates to groupBy 2nd column
        groupRDD.foreach(println)
        val groupByRDD = mapRDD2.map(x=>(x._2,x._1)).groupByKey()// assumes the first "column" is the join key.
        println("******************************************************************")
        println("GROUP RDD after performing groupByKey Transformation")
        groupByRDD.foreach(println)
        println(groupByRDD.count())
        println("******************************************************************")
        /** 6) aggregateByKey */
        /** When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key
         * are aggregated using the given combine functions and a neutral "zero" value. Allows an aggregated value type
         * that is different than the input value type, while avoiding unnecessary allocations. Like in groupByKey,
         * the number of reduce tasks is configurable through an optional second argument. More details read Method used
         * Syntax will be like this aggregateByKey(zeroVal)(seqOp, combOp) */
        //Zero Value: Zero value in our case will be 0 as we are finding Maximum Marks
        val zeroVal = 0
        //println("******************************************************************")
        //println("AGGREGATE BY RDD after performing aggregateBy Transformation")
        //aggregate() also same as aggregateByKey() except for aggregateByKey() operates on Pair RDD
        // val agreeRDD = agrregateRelatedMethod().map(t => (t._1, (t._2, t._3))).aggregate(zeroVal)(seqOp, combOp)
        //_.2 indicates
        // to groupBy 2nd column
        //agreeRDD.foreach(println)
        val aggrRDD = agrregateRelatedMethod().map(t => (t._1, (t._2, t._3))).aggregateByKey(zeroVal)(seqOp, combOp)
        //The below result set depicts the maximum marks scored by a student among all his subjects.
        println("******************************************************************")
        println("AGGREGATE RDD after performing aggregateByKey Transformation")
        aggrRDD.collect.foreach(println)
        println("******************************************************************")

        /** 7) sortBY and sortByKey */
        /** When called on a dataset of (K, V) pairs where K implements Ordered, returns a dataset of (K, V) pairs
         * sorted by keys in ascending or descending order, as specified in the boolean ascending argument.
         * Syntax: def sortBy[B](f: A => B)(implicit ord: Ordering[B]): Repr
         */
        println("******************************************************************")
        println("SORT RDD in ASC after performing sortBy Transformation")
        val sortRDD=reduceRDD.sortBy(p=>p._2) //_.2 indicates to sortBy 2nd column
        sortRDD.foreach(println)
        val sortByRDD=reduceRDD.map(x=>(x._2,x._1)).sortByKey(false) // use false for DESC order, true for ASC order
        //treats first column as KEY to sort by ..we cannot supply a specif colun to sort using sortBykey
        println("******************************************************************")
        println("SORT RDD in DESC after performing sortByRDD Transformation")
        sortByRDD.collect.foreach(println)
        println("******************************************************************")

        /** 8) JOIN or INNER JOIN*/
        /** When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs
         * of elements for each key. Outer joins are supported through leftOuterJoin, rightOuterJoin, and fullOuterJoin.
         FOR only DF/DS: Any type of join can be performed based on common key just like in sql a.id===b.id..which
        picks only common records..below join doesn't provide any col/key to join so it includes all possibilities
        for each key
         */

        println("******************************************************************")
        println("JOIN RDD after performing join Transformation")
        val joinNamesRDD = reduceRDD.join(reduceRDD2) //join assumes the first "column" is the join key.
        joinNamesRDD.take(3).foreach(println)
        println("Count: "+joinNamesRDD.count())
        println("******************************************************************")
        val flipKeyRDD=reduceRDD.map(y=>(y._2,y._1))
        val flipKey2RDD=reduceRDD2.map(y=>(y._2,y._1))
        val joinKeyRdd=flipKey2RDD.join(flipKeyRDD,10)
          // for dataframes we can specify column
        println("JOIN RDD after performing join flipping columns to get key as first column")
        joinKeyRdd.take(3).foreach(println)
        println("Count: "+joinKeyRdd.count())
        println("******************************************************************")
        println("LEFT OUTER JOIN RDD")
        //The LEFT OUTER JOIN returns the dataset that has all rows from the left dataset,
        // and the matched rows from the right dataset.(based on the condition cols provided).
        //and for the non-matching values for left ..null values are appended on the right dataset in join RDD
        val leftOuterJoinRDD=reduceRDD.leftOuterJoin(reduceRDD2)
        leftOuterJoinRDD.take(3).foreach(println)
        println("Total Count: "+leftOuterJoinRDD.count())
        println("******************************************************************")
        println("LEFT OUTER JOIN RDD AFTER FLIP OF KEYS")
        val leftOuterJoinFlipRDD=flipKey2RDD.leftOuterJoin(flipKeyRDD,10)
        leftOuterJoinFlipRDD.take(3).foreach(println)
        println("Total Count: "+leftOuterJoinFlipRDD.count())
        println("******************************************************************")
        println("RIGHT OUTER JOIN RDD")
        //The RIGHT OUTER JOIN returns the dataset that has all rows from the right dataset,
        // and the matched rows from the left dataset.
        //and for the non-matching values for right ..null values are appended on the left dataset in join RDD
        val rightOuterJoinRDD = reduceRDD.rightOuterJoin(reduceRDD2)
        rightOuterJoinRDD.take(3).foreach(println)
        println("Total Count: " + rightOuterJoinRDD.count())
        println("******************************************************************")
        println("RIGHT OUTER JOIN RDD AFTER FLIP OF KEYS")
        val rightOuterJoinFlipRDD = flipKey2RDD.rightOuterJoin(flipKeyRDD, 10)
        rightOuterJoinFlipRDD.take(3).foreach(println)
        println("Total Count: " + rightOuterJoinFlipRDD.count())
        println("******************************************************************")
        println("FULL OUTER JOIN RDD")
        //The FULL OUTER JOIN returns the dataset that has all rows when there is a match in either the left or right dataset.
        //and the corresponding non-matching in both left or right has null values for it
        val fullOuterJoinRDD = reduceRDD.fullOuterJoin(reduceRDD2)
        fullOuterJoinRDD.take(3).foreach(println)
        println("Total Count: " + fullOuterJoinRDD.count())
        println("******************************************************************")
        println("FULL OUTER JOIN RDD AFTER FLIP OF KEYS")
        val fullOuterJoinFlipRDD = flipKey2RDD.fullOuterJoin(flipKeyRDD, 10)
        fullOuterJoinFlipRDD.take(3).foreach(println)
        println("Total Count: " + fullOuterJoinFlipRDD.count())

        /** 8) COALESCE */// we can consider this as a NARROW TRANSFORMATION
        /** Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more
         * efficiently after filtering down a large dataset.
         * AVOIDS shuffle - by merging the PARTITIONS on the same machine/node/worker
         * partitions will be of uneven size since shuffle is avoided
         * Used to decrease NUMBER of Partitions
         */
        println("******************************************************************")
        println("Number of Partitions before COALESCE: "+joinKeyRdd.getNumPartitions)
        val joinKeyCoascleRDD=joinKeyRdd.coalesce(2)
        println("Number of Partitions after COALESCE: "+joinKeyCoascleRDD.getNumPartitions)
        println("******************************************************************")

        /** 9) REPARTITION */
        /** Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them.
         *  This always shuffles all data over the network.
         *  Better parallelism compared to coalesce and tries to evenly distribute data across the partitions
         *  will create partitions almost similar in size
         *  PERFORMS shuffle(full shuffle)
         *  Used to increase or decrease NUMBER of Partitions
         */
        println("Number of Partitions before REPARTITION: " + joinKeyRdd.getNumPartitions)
        val joinKeyRepartitionRDD = joinKeyRdd.repartition(3)
        println("Number of Partitions after REPARTITION: " + joinKeyRepartitionRDD.getNumPartitions)
        println("******************************************************************")

        /**10) repartitionAndSortWithinPartitions */
       /** Repartition the RDD according to the given partitioner and, within each resulting partition,
        * sort records by their keys. This is more efficient than calling repartition and then sorting within each
        * partition because it can push the sorting down into the shuffle machinery. */

        /**11) combineBykey  */ //similar to aggregate By
        //more about reduce,fold and scan - https://www.youtube.com/watch?v=W1VLzv66dkU
        /**12) foldByKey  */   //

        /**13) CARTESIAN
         * When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements).
         *
         */

        /**14) PIPE
         * Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script.
         * RDD elements are written to the process's stdin and lines output to its stdout are returned as an RDD of strings.
         * used to perform some linux commands on a dataframe/RDD/DS
         * eg: val pipeRDD= rdd.pipe(some linux commands script)
         *  */

        /**15) COGROUP
         * When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (Iterable<V>, Iterable<W>)) tuples.
         * This operation is also called group With. */

        /** ---------------------WIDE TRANSFORMATIONS ---------------------- */
      }  // WIDE TRANFORMATION IF LOOP END

    val actions=true
    if(actions){

    }


    /** FUNCTIONS THAT CAN BE USED ON RDD */
    val functionsTest = false
    if (functionsTest) {
      someRDDMeth().foreach(println) /** to print all rows in the RDD one row in each line */

      someRDDMeth().getNumPartitions /** to get NUMBER of partitions the RDD has */

      someRDDMeth().collect() /** It is an *ACTION* can be applied RDD/Dataframe and this converts all the rows of the
       *  RDD to a collection (Array[Row]). It is used useful in retrieving all the elements of the row from each partition
       *  in an RDD and brings that over the driver node/program.Be careful when you use this action when you
       *  are working with huge RDD with millions and billions of data as you may run out of memory on the driver. */
      df1.collectAsList()/** ACTION function is similar to collect() but it returns Java util list.
       ** can ONLY be applied to DATAFRAME/DATASET but not RDD**
       *  collect() : scala.Array[T]
       *  collectAsList() : java.util.List[T] */


    }
    //to view spark uI from intellij use below which holds the UI until we input something
  //  val a = scala.io.StdIn.readInt()
    //println("The value of a is " + a)

  }

  /** RDD CREATION WAYS */
/**Spark RDD can be created in several ways using Scala & Pyspark languages, for example,
It can be created by using sparkContext.parallelize(), from text file, from another RDD, DataFrame, and Dataset.*/
// 1) any collection passed via parallelize method becomes RDD
  //2) Reading from external files
  //3) from existing RDD
  //4) from existing dataframes or datasets using .rrd method . Eg: df.rdd provides an rdd from df

  /**1) creatingRDDUsingParallelize*/
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
  /** Both textFiles and wholetextfiles can be used to read multiple files. for difference read methods
  //and we optionally provide the NUM of PARTITIONS if we wish. Just put (filepath/Array,NumOfPartitions)
  //Besides text file we can use any other file like JSON,CSV etc..to create RDD*/
  /** 2)creatingRDDUsingAnyFile */
 // sparkContext.textFile() method is used to read a text file from HDFS,
 // S3 and any Hadoop supported file system, this method takes the path as an argument and optionally takes a number of partitions
 // as the second argument.
  //will not create pairedRDD ..so no filename/filepath  inside it
def creatingRDDUsingTextFile(pathToLoad:String): RDD[String] ={
  val someRDD=Util.spark.sparkContext.textFile(s"$pathToLoad")
  someRDD
}

  //sparkContext.wholeTextFiles() reads a text file into PairedRDD of type RDD[(String,String)]
  // with the key being the file path and value being contents of the file.
  // This method also takes the path as an argument and optionally takes a number of partitions as the second argument.
  //you will get a resulting RDD with first element = filepath and second element = content
  def creatingRDDUsingTextFiles(pathToLoad:String): RDD[(String,String)] ={
    val someRDD=Util.spark.sparkContext.wholeTextFiles(s"$pathToLoad")
    someRDD
  }

  /**3) from existing RDD by applying any transformation */

  /**4) from existing Dataframe or Dataset using .rdd method : refer someRDDMeth below for more details*/



    /**------------- SOME USEFUL METHODS TO PERFORM ABOVE OPERATIONS -------- */

  /** REMOVE FIRST ROW FROM RDD */
    def removefirstRow(data:RDD[(String,String)]):RDD[(String,String)]={
  val header = data.first()
  val data2= data.filter(row => row != header)
  data2
}
  /** remove first two characters from each record/row in RDD */
  def removeFirstTwoRows(): Unit ={
    val removeFirstCharsRDD=someRDDMeth().map(x => x.mkString.substring(2))
     removeFirstCharsRDD.foreach(println)
  }
/** SPLIT */
  def splitRDD(data:RDD[(String,String)]):RDD[String]={
    val data2= data.flatMap( e=>e._2.split("[?;\\n]+")).filter(_.nonEmpty)

    data2
  }
/** CREATING RDD FROM DATAFRAME */
  def someRDDMeth() : RDD[Row]={

    val structureData = Seq(
      Row("James ", "", "Smith", "36636", "M", 3100),
      Row("Michael ", "Rose", "", "40288", "M", 4300),
      Row("Robert ", "", "Williams", "42114", "M", 1400),
      Row("Maria ", "Anne", "Jones", "39192", "F", 5500),
      Row("Jen", "Mary", "Brown", "", "F", -1)
    )
    val structureSchema = new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType) //here the name struct ends
      .add("id", StringType)
      .add("gender", StringType)
      .add("salary", IntegerType)

    val someRDD = spark.createDataFrame(spark.sparkContext.parallelize(structureData), structureSchema).rdd

    someRDD
  }

  def rddForUnionByName():(RDD[Row],RDD[Row],RDD[Row],RDD[Row],DataFrame,DataFrame) ={
    //Create DataFrame df1 with columns name,dept & age
    val data = Seq(("James", "Sales", 34), ("Michael", "Sales", 56),
      ("Robert", "Sales", 30), ("Maria", "Finance", 24))
    import Util.spark.implicits._
    val df1 = data.toDF("name", "dept", "age")
    //Create DataFrame df1 with columns name,dep,state & salary
    val data2 = Seq(("James", "Sales", "NY", 9000), ("Maria", "Finance", "CA", 9000),
      ("Jen", "Finance", "NY", 7900), ("Jeff", "Marketing", "CA", 8000))
    val df2 = data2.toDF("name", "dept", "state", "salary")
    val rdd1:RDD[Row]=df1.rdd
    val rdd2:RDD[Row]=df2.rdd
    val merged_cols = df1.columns.toSet ++ df2.columns.toSet
    import org.apache.spark.sql.functions.{col, lit}
    def getNewColumns(column: Set[String], merged_cols: Set[String]) = {
      merged_cols.toList.map(x => x match {
        case x if column.contains(x) => col(x)
        case _ => lit(null).as(x)
      })
    }

    val new_df1 = df1.select(getNewColumns(df1.columns.toSet, merged_cols): _*)
    val new_df2 = df2.select(getNewColumns(df2.columns.toSet, merged_cols): _*)
    val new_rdd1: RDD[Row] = new_df1.rdd
    val new_rdd2: RDD[Row] = new_df2.rdd
    (rdd1, rdd2, new_rdd1, new_rdd2,df1,df2)
  }

/** ITERATOR */
  def aboutIterator(): Unit ={
    val v = Iterator(5, 1, 2, 3, 6, 4)

    //checking for availability of next element
    while(v.hasNext)

    //printing the element
      println(v.next)

    //We can define an iterator for any collection(Arrays, Lists, etc) and
    // can step through the elements of that particular collection.
    val someArray = Array("A", "B", 2, 3)
    //val v = List(5,1,2,3,6,4)

    // defining an iterator
    // for a collection
    val i = someArray.iterator

    while (i.hasNext)
      print(i.next + " ")

    //Using built-in functions min and max Iterators can be traversed only once.
    // Therefore, we should redefine the iterator after finding the maximum value.
    // defining iterator
    val i1 = Iterator(5, 1, 2, 3, 6, 4)

    // calling max function
    println("Maximum: " + i1.max)

    // redefining iterator
    val i2 = Iterator(5, 1, 2, 3, 6, 4)

    // calling min function
    println("Minimum: " + i2.min) //if we do not redefine iterator and directly call i1.min then we get
                                  // "Exception in thread "main" java.lang.UnsupportedOperationException: empty.min"
  }
/** -----------------aggregateByKey ---------------------*/
  def agrregateRelatedMethod(): RDD[(String,String,Int)] ={
    /** aggregateByKey function in Spark accepts a total of three parameters
     * for more operations follow the below link
    https://www.projectpro.io/recipes/explain-aggregatebykey-spark-scala
    1. Initial value or Zero value
       a.It can be 0 if aggregation is a type of sum of all values
       b.We have had this value as Double.MaxValue if aggregation objective is to find the minimum value
       c.We can also use Double.MinValue value if aggregation objective is to see maximum value.
         Or we can also have an empty List or Map object if we want a respective collection as an output for each key
    2.  Sequence operation function transforms / merges data of one type[V] to another type[U].
    3.The combination operation function merges multiple transformed types[U] to a single type[U].*/
    // Creating PairRDD studentRDD with key value pairs, Number partitions is 3 defined in parallelize method.
    val studentRDD = Util.spark.sparkContext.parallelize(Array(
      ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82),
      ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80),
      ("Tina", "Maths", 78), ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
      ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74),
      ("Cory", "Maths", 56), ("Cory", "Physics", 65), ("Cory", "Chemistry", 71), ("Cory", "Biology", 68),
      ("Jackeline", "Maths", 86), ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83),
      ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)), 3)
    studentRDD
  }
  //Sequence operation : Finding Maximum Marks from a single partition
  def seqOp = (accumulator: Int, element: (String, Int)) =>
    if (accumulator > element._2) accumulator else element._2

  //Combiner Operation : Finding Maximum Marks out Partition-Wise Accumulators
  def combOp = (accumulator1: Int, accumulator2: Int) =>
    if (accumulator1 > accumulator2) accumulator1 else accumulator2

  /** -----------------aggregateByKey ---------------------*/

}

/**
//RDD ACTIONS
//1)collect

IMP POINTS
RDD (Resilient Distributed Dataset)
RDD (Resilient Distributed Dataset) is a fundamental data structure of Spark and it is the primary data abstraction in Apache Spark and the Spark Core.
RDDs are fault-tolerant, immutable distributed collections of objects, which means once you create an RDD you cannot change it.
Each dataset in RDD is divided into logical partitions, which can be computed on different nodes of the cluster.

RDD (Resilient Distributed Dataset) is the fundamental data structure of Apache Spark which are an
immutable distrubuted collection of objects which computes on the different node of the cluster.
Each and every dataset in Spark RDD is logically partitioned across many servers so that
they can be computed on different nodes of the cluster.

A data structure is a particular way of organizing data in a computer so that it can be used effectively.
Data abstraction is the reduction of a particular body of data to a simplified representation of the whole

**In other words, RDDs are a collection of objects similar to collections in Scala,
with the difference being RDD is computed on several JVMs scattered across multiple physical
servers also called nodes in a cluster while a Scala collection lives on a single JVM.**

Additionally, RDDs provide data abstraction of partitioning and distribution of the data
which is designed to run computations in parallel on several nodes, while doing transformations on RDD
most of the time we don’t have to worry INTRODUCTION parallelism as Spark by default provides it.

RDD Advantages  //////

– In-Memory Processing
In Apache Spark, In-memory computation defines as instead of storing data in some
slow disk drives the data is kept in random access memory(RAM).
Also, that data is processed in parallel.
By using in-memory processing, we can detect a pattern, analyze large data.

– Immutability
Once an RDD is created it cannot be modified. Any transformation on it will create a new RDD.
 If a worker node goes down, using Lineage(DAG)
 (a track of all the transformations that has to be applied on that RDD including from where it has to read the data)
 we can re-compute the lost partition of RDD from the original one.

– Fault Tolerance
RDDs are resilient and can recompute missing or damaged partitions for a complete recovery if a node fails.

– Lazy Evolution
The data inside the RDD is not evaluated until an action is triggered for computation.

– Partitioning
RDDs are divided into smaller chunks called partitions (logical chunks of data),
when some actions are executed, a task is launched per partition.
The number of partitions are directly responsible for parallelism.

– Parallelize  (is not an action)
parallelize() method is the SparkContext's parallelize method to create a parallelized collection. T
his allows Spark to distribute the data across multiple nodes,
instead of depending on a single node to process the data
////

parallelize in the sense partitioning the data or breaking the data into logical chunks
defualt number of partitions for an RDD is 1
if you read n files into an RDD then the number of partitions for that RDD will be n

//collect – Returns all data from RDD as an array.
show() : It will show only the content of the dataframe.
df. collect() : It will show the content and metadata of the dataframe.
df.take() : shows content and structure/metadata for a limited number of rows for a very large dataset.

more INTRODUCTION RDD creation
https://sparkbyexamples.com/spark-rdd-tutorial/

RDD.saveAsObjectFile and SparkContext.objectFile support saving an RDD in a simple format consisting of serialized Java objects.
While this is not as efficient as specialized formats like Avro, it offers an easy way to save any RDD.

 more about RDD's: types of RDD's
 https://blog.knoldus.com/rdd-sparks-fault-tolerant-in-memory-weapon/#:~:text=Coarse%2Dgrained%20transformations%20are%20those,than%20a%20coarse%20grained%20one.

 */