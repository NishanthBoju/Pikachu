import org.apache.avro.Schema
import Util.spark
import java.io.File

object ReadingAllFileTypes {


  def main(args: Array[String]):Unit={
      /** *******************************AVRO****************************** **/
      /** NOTE**: instead of com.dtabricks avro we can use sql.avro but make sure to use same to
       * read/write to avoid write errors
       */
      //AVRO: ROW - BASED DATA FORMAT
      // Two ways to create a Dataframe on top of AVRO files.

      /** Method 1: to take schema directly from the files * */
      //need maven -com.databricks.spark.avro dependency (org.apache.spark:spark-avro_2.12:2.4.4)
      // we need spark-avro-jar locally to import databricks avro on spark-shell(spark-shell --jars spark-avro_2.12:2.4.4.jar)
      //creates dataframe on top of avro files and gets schema from the files itself
          if(false) {
            val avroWithoutSchemaDF = spark.read.format("com.databricks.spark.avro").load("/Users/new/IdeaProjects/Pikachu/src/main/resources/AVRO/*")

            /** M E THOD 2: externally providing schema file and create dataframe on top of the avro files using this schema* */
            //providing schema file which is located on LOCAL not HDFS
            //Schema.Parser : import org.apache.avro.Schema
            //File:  import java.io.File
            val schemaAvro = new Schema.Parser().parse(new File("/Users/new/IdeaProjects/Pikachu/src/main/resources/AVRO/schema/schema.avsc"))

            //providing data files at any LOCATION
            val baseSchemaDF = spark.read
              .format("com.databricks.spark.avro")
              .option("avroSchema", schemaAvro.toString)
              .load("/Users/new/IdeaProjects/Pikachu/src/main/resources/AVRO/*")

            baseSchemaDF.show()
            //To write a dataframe into avro files
            baseSchemaDF.write.format("com.databricks.spark.avro").save("/Users/new/IdeaProjects/Pikachu/src/main/resources/AVRO/samplewrite")
            //To write a dataframe into avro files using partitions. we can input multiple partitions
            baseSchemaDF.write.partitionBy("birthdate", "id").format("com.databricks.spark.avro").save("/Users/new/IdeaProjects/Pikachu/src/main/resources/AVRO/samplewritewithparttions.avro")
          }
      /** *******************************AVRO****************************** */

    /*********************************PARQUET*******************************/

    if(false){
      /** Encoders for most common types are automatically provided by importing spark.implicits._ */
      import spark.implicits._

      val peopleDF = spark.read.format("com.databricks.spark.avro").load("/Users/new/IdeaProjects/Pikachu/src/main/resources/AVRO/userdata1.avro")
      peopleDF.show()

      // DataFrames can be saved as Parquet files, maintaining the schema information
      peopleDF.write.parquet("Users/new/IdeaProjects/Pikachu/src/main/resources/PARQUET" +
        "/samplewrite")

      // Read in the parquet file created above
      // Parquet files are self-describing so the schema is preserved
      // The result of loading a Parquet file is also a DataFrame
      val parquetFileDF = spark.read.parquet("Users/new/IdeaProjects/Pikachu/src/main/resources/PARQUET" +
        "/samplewrite")
      parquetFileDF.show()
     /** Spark provides the capability to append DataFrame to existing parquet files using “append” save mode.
      In case, if you want to overwrite use “overwrite” save mode.  */
      parquetFileDF.write.mode("append").parquet("Users/new/IdeaProjects/Pikachu/src/main/resources/PARQUET" +
        "/samplewrite")

      /** to create partitions while writing  */
      parquetFileDF.write.partitionBy("id", "firstname")
        .parquet("/tmp/output/people2.parquet")

    }


    /*********************************PARQUET*******************************/

    //SAME above CONVENTIONS can be used for any file formats
  }

}
