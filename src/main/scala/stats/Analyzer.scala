package stats
import to_pair._
import averaging._

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Rating(user: Int, item: Int, rating: Double)

/*
Create extension methods for  :
* user & rating
* item & rating
And compute global average
 */
object to_pair {

  implicit class paired_functions (data : RDD[Rating]) {
    def user_rating : RDD[(Int,Double)] = data.map(r=>(r.user,r.rating))
    def item_rating : RDD[(Int,Double)]  = data.map(r=>(r.item,r.rating))
    def rating_avg : Double  = data.map(_.rating).mean
  }
}
/*
Create extension methods for :
* per user and per item average
* all user and all item close to global average
* ratio of close users and close items to global average
* average of per user average and per item average
* per user average and per item average min and max
 */

object averaging {
  implicit class average_functions (data : RDD[(Int,Double)]){
    /* Compute average per key of rdd

     */
    def feature_average : RDD[ (Int, Double)] = {
      data.aggregateByKey((0.0, 0))(
        (k, v) => (k._1 + v, k._2 + 1),
        (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
        .mapValues(sum => 1.0 * sum._1 / sum._2.toDouble)}

    /*
    * Check if min and max value are close to global average given threshold
    * @param : global average, threshold
    * @return : True if all min and max of grouped by key rdd are close to global average
    */

    def all_close (global_average: Double, threshold  : Double=0.5)  = {
      ((data.min_value - global_average).abs < threshold) && ((data.max_value-global_average).abs < threshold)

    }
    /*
    * Return ratio of keys with average close to global average
    * @param : global average, threshold
    * @return ratio
     */

    def ratio(global_avg: Double,threshold : Double =0.5) : Double = {
      1.0 * data.filter(r=> (r._2-global_avg).abs < threshold).count / data.count()}

    def average : Double = data.map(r=>r._2).mean

    def min_value : Double = {
      data.map(r=>r._2).min
    }
    def max_value : Double = {
      data.map(r=>r._2).max
    }

  }

}

object Analyzer extends App {
  // Remove these lines if encountering/debugging Spark
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR") 

  println("")
  println("******************************************************")

  var conf = new Conf(args) 
  println("Loading data from: " + conf.data()) 
  val dataFile = spark.sparkContext.textFile(conf.data())
  val data = dataFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  }) 
  assert(data.count == 100000, "Invalid data")

  // Code for understanding the difference in prediction accuracy between user based and item abased average

  // Save answers as JSON
  def printToFile(content: String, 
                  location: String = "./answers.json") =
    Some(new java.io.PrintWriter(location)).foreach{
      f => try{
        f.write(content)
      } finally{ f.close }
  }
  conf.json.toOption match {
    case None => ; 
    case Some(jsonFile) => {
      var json = "";
      {
        // Limiting the scope of implicit formats with {}
        implicit val formats = org.json4s.DefaultFormats
        val answers: Map[String, Any] = Map(
          "Q3.1.1" -> Map(
            "GlobalAverageRating" -> data.rating_avg // Datatype of answer: Double
          ),
          "Q3.1.2" -> Map(
            "UsersAverageRating" -> Map(
                // Using as your input data the average rating for each user,
                // report the min, max and average of the input data.
                "min" -> data.user_rating.feature_average.min_value,  // Datatype of answer: Double
                "max" -> data.user_rating.feature_average.max_value, // Datatype of answer: Double
                "average" -> data.user_rating.feature_average.average// Datatype of answer: Double
            ),
            "AllUsersCloseToGlobalAverageRating" -> data.user_rating.all_close(data.rating_avg), // Datatype of answer: Boolean
            "RatioUsersCloseToGlobalAverageRating" -> data.user_rating.feature_average.ratio(data.rating_avg) // Datatype of answer: Double
          ),
          "Q3.1.3" -> Map(
            "ItemsAverageRating" -> Map(
                // Using as your input data the average rating for each item,
                // report the min, max and average of the input data.
                "min" -> data.item_rating.feature_average.min_value,  // Datatype of answer: Double
                "max" -> data.item_rating.feature_average.max_value, // Datatype of answer: Double
                "average" -> data.item_rating.feature_average.average // Datatype of answer: Double
            ),
            "AllItemsCloseToGlobalAverageRating" -> data.item_rating.all_close(data.rating_avg), // Datatype of answer: Boolean
            "RatioItemsCloseToGlobalAverageRating" -> data.item_rating.feature_average.ratio(data.rating_avg) // Datatype of answer: Double
          ),
         )
        json = Serialization.writePretty(answers)
      }

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  println("")
  spark.close()
}
