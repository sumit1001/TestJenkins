
import org.apache.spark._
import org.apache.spark.sql._

object SparkSQLFirstJob {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("test").setMaster("local");
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    var data = spark.read.textFile("file:\\c:\\Temp\\APD_Incident_Extract_YTD.txt")
    .map(_.split(",")).map(attributes => 
      CrimeData(attributes(0), attributes(1)
        ,attributes(2),attributes(3),
        attributes(4))).toDF

     data.printSchema()
     data.createTempView("CrimeData")
     
     spark.sql("select count(1) from CrimeData").show()
     println(data.count())
     
     println("Simple Filter " + data.filter("CrimeType == 'ROBBERY BY THREAT'").count())
     
    
     var filterData = 
       data.groupBy("CrimeType").count()
     filterData.printSchema
    filterData.sort($"count".desc).show(500)
     
     //col("CrimeType").equals("ROBBERY BY THREAT")
     //.===("ROBBERY BY THREAT")
    
    //data.
    //col("c1").as("Incident Report Number")
    
    
    //data.select("name", "age").show()

  }
  
  
}

case class CrimeData(IncidentNumber:String,CrimeType:String,Date:String,
    Time:String,Location_type:String)
