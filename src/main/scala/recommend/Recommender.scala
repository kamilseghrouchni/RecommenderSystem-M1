package recommend

import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import predict.Predictor
import stats.Rating

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val data = opt[String](required = true)
  val personal = opt[String](required = true)
  val json = opt[String]()
  verify()
}

case class Prediction(item: Int, title: String, rating: Double)
object Recommender extends App {
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

  assert(data.count == 100000, " Invalid  data")

  val Array(pre_test, pre_train) = data.randomSplit(Array(0.2, 0.8),5)



  println("Loading personal data from: " + conf.personal())

  val personalFile = spark.sparkContext.textFile(conf.personal())
  assert(personalFile.count == 1682, "Invalid personal data")
  val predict = spark.sparkContext.emptyRDD[Rating]

 val personal = personalFile.map(l => {
   val columns = l.split(",").map(_.trim)

   // extract both rated and unrated movies
  columns.length match {
    case 3 => (columns(0).toInt,columns(1),columns(2).toDouble)
    case 2 =>(columns(0).toInt,columns(1),-999.0)
  }


 })

  // create train subset with rated movies for user id 944
  val user_rating =personal.filter(r=> r._3 != -999.0).map(r=> Rating(944,r._1,r._3))
  // create test set with movie without ratings (to predict)
  val test =personal.filter(r=> r._3 == -999.0).map(r=> Rating(944,r._1,r._3))
 // add user id 944 ratings to train data set
  val train = data.union(user_rating)
  // compute baseline prediction on train test
  val prediction = Predictor.baseline_predictor(train,test)
  // get movie id and title
  val id_movie = personal.map(r=>(r._1,r._2))
  /*
  *ranked in ascending order for movie identifier
  *ranked in descending order for movie rating
  *format tailored for json save
   */
  val top_5 = prediction.map{case ((_,item),rating)=> (item,rating)}
    .join(id_movie)
    .sortByKey(ascending = true)
    .map{case(item,(rating,title))=> (item,title,rating)}
    .sortBy(_._3,ascending = false)
    .take(5)
    .map(r=> List(r._1,r._2,r._3))
    .toList
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

            // IMPORTANT: To break ties and ensure reproducibility of results,
            // please report the top-5 recommendations that have the smallest
            // movie identifier.

            "Q4.1.1" -> top_5
         )
        json = Serialization.writePretty(answers)
      }

      println(json)
      println("Saving answers in: " + jsonFile)
      printToFile(json, jsonFile)
    }
  }

  spark.close()
}
