import Applications._
import Util.spark
import org.apache.spark.sql.DataFrame
object main {

  def main(args:Array[String]) : Unit = {
  // def to define a method
    // main is the name of the Main Method
    //(args: Array[String]) is INTRODUCTION method arguments that tell the compiler what type of argument it can accept during the call.
    //Unit is the return type of the Main method
    val csv10DF = spark.read.format("csv").option("header", true).load("/Users/new/IdeaProjects/Pikachu/src/main/resources/CSV/Sales_Records.csv").limit(10)

    //TRANSFORMATIONS
    //1.1 MAP
    // map takes a function and collection as input and
    // returns a new collection with the given function applied over each element of collection
    val mapDF=csv10DF




  }

}
