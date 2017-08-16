import org.apache.spark._

object SparkFirstJob {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("test").setMaster("local");
    val sc = new SparkContext(conf)

    var file = "file:\\c:\\Temp\\test.txt"

    val data = sc.textFile(file).flatMap { x => x.split(" ") }.map { x => (x, 1) }

    val outcome = data.reduceByKey((a, b) => a + b).collect()
    println("here is output1")

    println(outcome.foreach(f => println(f._1 + " = " + f._2)))
    println("here is output1")

  }
}