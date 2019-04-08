import org.apache.spark._
import org.apache.spark.SparkContext._

object WordCount {
    def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("Word Count")
      // Make a SparkContext
      val sc = new SparkContext(conf)
      //Store content of "input.txt" into SparkContext
      val input =  sc.textFile("input.txt")
      //Tokenize the words using " " as the delimiter
      val words = input.flatMap(line => line.split(" "))
      //Count the words and take the total sum
      val rdd = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      // Save into specified file
      rdd.saveAsTextFile("output")
    }
}
