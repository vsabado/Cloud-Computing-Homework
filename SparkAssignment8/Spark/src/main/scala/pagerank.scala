import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object pagerank {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Page Rank")
    val input = new SparkContext(conf).textFile("input.txt").cache().filter(!_.isEmpty)
    val iters = if (args.length > 0) args(0).toInt else 10
    println("Number of iterations to do:  " + iters)
    val edges = input.map { p =>
      val tokens = p.split(" ")
      (tokens(0), tokens(1))
    }

    val connectionsReducedByKey = edges.distinct().groupByKey()
    val largestValue = edges.values.max().toInt
    val largestKey = edges.keys.max().toInt
    val maxNode = if (largestKey > largestValue) largestKey else largestValue

    //    println("Size by value: " + largestValue)
    //    println("Size by key" + largestKey)
    //println("Larger value: " + maxNode)

    val alpha = 0.10
    var ranks = connectionsReducedByKey.mapValues(v => 1.0 / maxNode)
    println("Initial values: ")
    ranks.foreach(println)

    val weights = connectionsReducedByKey.join(ranks).flatMap {
      case (_, (pageConnections, rank)) =>
        val size = pageConnections.size
        pageConnections.map(dest => (dest, rank / size))
    }
    println(weights.countByValue())
    val danglingWeight = 1 - weights.values.sum()
    //println(danglingWeight)
    ranks = weights.reduceByKey((x, y) => x + y).mapValues(value => (alpha / maxNode) + (1.0 - alpha) * ((danglingWeight / maxNode) + value))
    ranks.foreach(println)
    for (i <- 2 to iters) {
      println(ranks.countByValue())
      val danglingWeight = 1 - ranks.values.sum()
      println("Dangling weight: " + danglingWeight)
      ranks = ranks.reduceByKey((x, y) => x + y).mapValues(value => (alpha / maxNode) + (1.0 - alpha) * ((danglingWeight / maxNode) + value))
      println(s"Ranks after iteration $i: ")
      ranks.foreach(println)
    }
    ranks.saveAsTextFile("output")
  }
}

