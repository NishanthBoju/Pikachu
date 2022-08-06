object Transformations {

  /*TWO TYPES OF TRANSFORMATIONS
  1)NARROW TRANSFORMATION
  Narrow transformations are the result of map() and filter() functions and these compute data that live on a single partition meaning there will not be any data movement between partitions to execute narrow transformations.
  Functions such as map(), mapPartition(), flatMap(), filter(), union() are some examples of narrow transformation

  2)Wider Transformation
  Wider transformations are the result of groupByKey() and reduceByKey() functions and
  these compute data that live on many partitions meaning there will be data movements between partitions to execute
  wider transformations. Since these shuffles the data, they also called shuffle transformations.
  Functions such as groupByKey(), aggregateByKey(), aggregate(), join(), repartition() are some examples of a wider transformations.

  Note: When compared to Narrow transformations, wider transformations are expensive operations due to shuffling.

TRANSFORMATION METHODS	    METHOD USAGE AND DESCRIPTION
cache()	                    Caches the RDD
filter()	                  Returns a new RDD after applying filter function on source dataset.
flatMap()                 	Returns flattern map meaning if you have a dataset with array, it converts each elements in a array as a row. In other words it return 0 or more items in output for each element in dataset.
map()	                      Applies transformation function on dataset and returns same number of elements in distributed dataset.
mapPartitions()           	Similar to map, but executes transformation function on each partition, This gives better performance than map function
mapPartitionsWithIndex()  	Similar to map Partitions, but also provides func with an integer value representing the index of the partition.
randomSplit()             	Splits the RDD by the weights specified in the argument. For example rdd.randomSplit(0.7,0.3)
union()	                    Combines elements from source dataset and the argument and returns combined dataset. This is similar to union function in Math set operations.
sample()                  	Returns the sample dataset.
intersection()             	Returns the dataset which contains elements in both source dataset and an argument
distinct()	                Returns the dataset by eliminating all duplicated elements.
repartition()             	Return a dataset with number of partition specified in the argument. This operation reshuffles the RDD randomly, It could either return lesser or more partitioned RDD based on the input supplied.
coalesce()                	Similar to repartition by operates better when we want to the decrease the partitions. Betterment achieves by reshuffling the data from fewer nodes compared with all nodes by repartition.
reduceByKey()               reduceByKey() merges the values for each key with the function specified. In our example, it reduces the word string by applying the sum function on value. The result of our RDD contains unique words and their count.
sortByKey()                 sortByKey() transformation is used to sort RDD elements on key. In our example, first, we convert RDD[(String,Int]) to RDD[(Int,String]) using map transformation and apply sortByKey which ideally does sort on an integer value.
                            And finally, foreach with println statement prints all words in RDD and their count as key-value pair to console.
intersection(otherDataset): Return a new RDD that contains the intersection of elements in the source dataset and the argument.
distinct([numPartitions])):	Return a new dataset that contains the distinct elements of the source dataset.
groupByKey([numPartitions]):	When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs.
                              Note: If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using reduceByKey or aggregateByKey will yield much better performance.
                              Note: By default, the level of parallelism in the output depends on the number of partitions of the parent RDD. You can pass an optional numPartitions argument to set a different number of tasks.
reduceByKey(func, [numPartitions]):	When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function func, which must be of type (V,V) => V. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.
aggregateByKey(zeroValue)(seqOp, combOp, [numPartitions]):	When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and a neutral "zero" value. Allows an aggregated value type that is different than the input value type, while avoiding unnecessary allocations. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.
sortByKey([ascending], [numPartitions]):	When called on a dataset of (K, V) pairs where K implements Ordered, returns a dataset of (K, V) pairs sorted by keys in ascending or descending order, as specified in the boolean ascending argument.
join(otherDataset, [numPartitions]):	When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key. Outer joins are supported through leftOuterJoin, rightOuterJoin, and fullOuterJoin.
cogroup(otherDataset, [numPartitions]):	When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (Iterable<V>, Iterable<W>)) tuples. This operation is also called groupWith.
cartesian(otherDataset):	When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements).
pipe(command, [envVars]):	Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script. RDD elements are written to the process's stdin and lines output to its stdout are returned as an RDD of strings.
coalesce(numPartitions):	Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more efficiently after filtering down a large dataset.
repartition(numPartitions):	Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them. This always shuffles all data over the network.
repartitionAndSortWithinPartitions(partitioner):	Repartition the RDD according to the given partitioner and, within each resulting partition, sort records by their keys. This is more efficient than calling repartition and then sorting within each partition because it can push the sorting down into the shuffle machinery.

   Action	                  Meaning
reduce(func)	Aggregate the elements of the dataset using a function func (which takes two arguments and returns one). The function should be commutative and associative so that it can be computed correctly in parallel.
collect()	Return all the elements of the dataset as an array at the driver program. This is usually useful after a filter or other operation that returns a sufficiently small subset of the data.
count()	Return the number of elements in the dataset.
first()	Return the first element of the dataset (similar to take(1)).
take(n)	Return an array with the first n elements of the dataset.
takeSample(withReplacement, num, [seed])	Return an array with a random sample of num elements of the dataset, with or without replacement, optionally pre-specifying a random number generator seed.
takeOrdered(n, [ordering])	Return the first n elements of the RDD using either their natural order or a custom comparator.
saveAsTextFile(path)	Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem, HDFS or any other Hadoop-supported file system. Spark will call toString on each element to convert it to a line of text in the file.
saveAsSequenceFile(path)
(Java and Scala)	Write the elements of the dataset as a Hadoop SequenceFile in a given path in the local filesystem, HDFS or any other Hadoop-supported file system. This is available on RDDs of key-value pairs that implement Hadoop's Writable interface. In Scala, it is also available on types that are implicitly convertible to Writable (Spark includes conversions for basic types like Int, Double, String, etc).
saveAsObjectFile(path)
(Java and Scala)	Write the elements of the dataset in a simple format using Java serialization, which can then be loaded using SparkContext.objectFile().
countByKey()	Only available on RDDs of type (K, V). Returns a hashmap of (K, Int) pairs with the count of each key.
foreach(func)	Run a function func on each element of the dataset. This is usually done for side effects such as updating an Accumulator or interacting with external storage systems.
Note: modifying variables other than Accumulators outside of the foreach() may result in undefined behavior. See Understanding closures for more details.

for various types of joins with examples
https://sparkbyexamples.com/spark/spark-sql-dataframe-join/








   */

  //MAP-Returns a new distributed dataset formed by passing each element of the source through a function func.


  /*
  * to use when otherwise and some other transformations
  https://sparkbyexamples.com/spark/spark-case-when-otherwise-example/
  * for equality andlogicoperations
  https://medium.com/@achilleus/a-practical-introduction-to-sparks-column-part-2-1e52f1d29eb1
  * scala transformations with examples
  https://supergloo.com/spark-scala/apache-spark-examples-of-transformations/
  *
  *difference between map and flatmap
  scala> sc.parallelize(List(1,2,3)).flatMap(x=>List(x,x,x)).collect
  res200: Array[Int] = Array(1, 1, 1, 2, 2, 2, 3, 3, 3)

  scala> sc.parallelize(List(1,2,3)).map(x=>List(x,x,x)).collect
  res201: Array[List[Int]] = Array(List(1, 1, 1), List(2, 2, 2), List(3, 3, 3))
  *
  *Window function
  https://sparkbyexamples.com/spark/spark-sql-window-functions/
  *
  *
  *
  *
  *
  *
  *
  *
  * */
}
