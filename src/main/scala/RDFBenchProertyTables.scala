import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File

object RDFBenchProertyTables {

  def main(args: Array[String]): Unit = {

    val t1 = System.nanoTime

    val conf = new SparkConf().setMaster("local").setAppName("SQLSPARK")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val filePathCSV:String =args(0)
    val filePathAVRO:String =args(1)
    val filePathORC:String =args(2)
    val filePathParquet:String =args(3)

    val csvFiles = new File(filePathCSV).list()


    csvFiles.foreach{
      PropertyTableName=>
        val PropertyTableName2=PropertyTableName.dropRight(4)
        val RDFPropertyTableDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(filePathCSV+"/"+PropertyTableName).toDF()

        RDFPropertyTableDF.write.format("com.databricks.spark.avro").save(filePathAVRO +"/"+PropertyTableName2+".avro")
        RDFPropertyTableDF.write.parquet(filePathParquet+"/"+PropertyTableName2+".parquet")
        RDFPropertyTableDF.write.orc(filePathORC+"/"+PropertyTableName2+".orc")

        println("Property Table: '" +PropertyTableName2+"' Has been Successfully Converted to AVRO, PARQUET and ORC !")
    }




/*
    val RDFDFJournal = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/PropertyTables/Journal.csv").toDF()

    RDFDFJournal.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/PropertyTables/Journal")
    RDFDFJournal.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFJournal.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")

    val RDFDFinProceedingArticle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/PropertyTables/inProceedingArticle.csv").toDF()

    RDFDFinProceedingArticle.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/PropertyTables/InProceedingArticle")
    RDFDFinProceedingArticle.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFinProceedingArticle.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")

    val RDFDFPerson = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/PropertyTables/person.csv").toDF()

    RDFDFPerson.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/PropertyTables/Person")
    RDFDFPerson.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFPerson.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")


    val RDFDFJournalArticle = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/PropertyTables/JournalArticle.csv").toDF()

    RDFDFJournalArticle.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/PropertyTables/JournalArticle")
    RDFDFJournalArticle.write.parquet("file:///C:/ExprimentRDFTest/Parquet/SingleTable")
    RDFDFJournalArticle.write.orc("file:///C:/ExprimentRDFTest/ORC/SingleTable")



    val RDFDFDocument = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///C:/ExprimentRDFTest/CSV/PropertyTables/Document.csv").toDF()

    RDFDFDocument.write.format("com.databricks.spark.avro").save("file:///C:/ExprimentRDFTest/AVRO/PropertyTables/Document")
    RDFDFDocument.write.parquet("file:///C:/ExprimentRDFTest/Parquet/Document")
    RDFDFDocument.write.orc("file:///C:/ExprimentRDFTest/ORC/Document")

*/


  }

}
