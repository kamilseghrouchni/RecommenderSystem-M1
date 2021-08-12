package predict
import stats.to_pair._
import stats.averaging._
import org.rogach.scallop._
import org.json4s.jackson.Serialization
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import stats.{Analyzer, Rating}

import time._


class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val train = opt[String](required = true)
  val test = opt[String](required = true)
  val json = opt[String]()
  verify()
}


/*
*Compute time taken by bloc of computation
*@param function to time
*@return List of 10 time measurements in miliseconds
 */
object Timer {
  def timer(t: => Any): IndexedSeq[Double]= {
    (1 to 10).map {_ =>
    val start = System.nanoTime()
    val func = t
    val end = System.nanoTime()
    (end - start) / 1e3
    }
  }
}

/*
*Extension methods to compute the statistics on time measurments
* @param : list of measurments in miliseconds
* compute min, max, average, and standard deviation
* */

object time {

  implicit class time_stats (times : IndexedSeq[Double]) {
    def min_times  : Double  =  Set(times).flatten.min
    def max_times  :Double  = Set(times).flatten.max
    def avg_times  :Double  = times.sum/times.length
    def stddev_times :Double  = scala.math.sqrt(times.map(r=> scala.math.pow((r-avg_times),2)).reduce(_+_)/times.length)
  }
}


object Predictor extends App {
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
  println("Loading training data from: " + conf.train())
  val trainFile = spark.sparkContext.textFile(conf.train())
  val train = trainFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(train.count == 80000, "Invalid training data")



  println("Loading test data from: " + conf.test())
  val testFile = spark.sparkContext.textFile(conf.test())
  val test = testFile.map(l => {
      val cols = l.split("\t").map(_.trim)
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble)
  })
  assert(test.count == 20000, "Invalid test data")

/*
* Compute mean absolute error
* @param rdd with rating and prediction of a per of user and item
* @return mean absolute error for the rdd */
  def mae (data : RDD[(Int,Int,Double,Double)]) : Double ={
    data.map(r=>scala.math.abs(r._4-r._3)).reduce(_+_) / data.count.toDouble
  }
/*
*scale specific to user average rating
*@param rating, user average rating
*@return scale specific rating
*/
    def scale (x : Double, user_avg : Double ) : Double = {
      x match {
        case a if x > user_avg  => (5.0 - user_avg)
        case b if x < user_avg  => (user_avg - 1.0)
        case c if x ==  user_avg  => 1.0
      }
    }

    /*
    *Compute global average on RDD
    * @param train rdd
    * @return global average
    *  */
    def global_avg_pred(train : RDD[Rating]) :  Double = {
      train.rating_avg
    }

  // time global average execution
    val time_global_avg_pred = Timer.timer(Predictor.global_avg_pred(train))

  // compute global average
    val global_average_v = Predictor.global_avg_pred(train)

  // compute global average prediction
    val mae_global_avg= test.map(r=>scala.math.abs(r.rating-global_average_v)).reduce(_+_) / test.count.toDouble


  /*
  *Compute item average prediction
  * @param train and test rdds
  * @return test rdd with extra column containing item average prediction RDD[(user, item,rating, item_average])
   */

    def item_avg_prediction(train : RDD[Rating],test : RDD[Rating]) :  RDD[(Int, Int, Double, Double)] = {
      val global_avg =  Predictor.global_avg_pred(train)
      val avg_item_rating = train.item_rating.feature_average

      test.map(r => (r.item, (r.user,r.rating)))
        .leftOuterJoin( avg_item_rating)
        .map { case (item, (user_rating, item_avg)) => (user_rating._1, item , user_rating._2 , item_avg.getOrElse(global_avg ))}
    }

  // time item average prediction
    val time_item_avg_pred =Timer.timer(item_avg_prediction(train,test).collect())

    val mae_per_item = mae(item_avg_prediction(train,test))


/*
*Compute user average prediction
* @param train and test rdds
* @return test rdds with extra column containing user average predicition RDD[(user, item,rating, user_average])
 */
    def user_avg_prediction (train : RDD[Rating],test: RDD[Rating] ):  RDD[(Int, Int, Double, Double)] = {
      val global_avg_ =  Predictor.global_avg_pred(train)
      val avg_user_rating =  train.user_rating.feature_average
      val prediction_pre = test.map(r => (r.user, (r.item,r.rating)))
        .join( avg_user_rating)
        .map { case (user, ((item,rating), prediction)) => (item ,user, rating , prediction)}
      assert ( prediction_pre.count == test.count)
      prediction_pre

    }


//  time user average prediction
    val time_user_avg_pred = Timer.timer(Predictor.user_avg_prediction(train,test).collect())



  val mae_per_user =Predictor.mae(user_avg_prediction(train,test))


  /*
*Compute baseline prediction
*@param train and test rdds, boolean = true in case bonus mode
*@retrun RDD [(user, item), baseline_prediction]
*/


  def baseline_predictor (train : RDD[Rating],test: RDD[Rating],bonus : Boolean=false):   RDD[((Int, Int), Double)] = {
    val avg_train =     Predictor.global_avg_pred(train)
    val avg_user_ratings =  train.user_rating.feature_average

    val normalized_deviation =train.map(r => (r.user, (r.item,r.rating)))
      .join (avg_user_ratings)
      .map { case (user, ((item,rating), avg_user)) => Rating(user,item,1.0*(rating-avg_user)/scale(rating,avg_user).toDouble)}

    val normalized_deviation_prep = normalized_deviation.item_rating
    val users = train.map(_.user).distinct.count
    val global_avg_pred=normalized_deviation_prep.aggregateByKey((0.0, 0))(
      (k, v) => (k._1 + v, k._2 + 1),
      (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .mapValues(sum => ( bonus match {
        case a if bonus => 1.0 * (sum._1+0.0013) / (sum._2.toDouble+users*0.0013)
        case b if !bonus => 1.0 * (sum._1) / (sum._2.toDouble)} ))


    val global_avg_rdd=Predictor.user_avg_prediction(train,test)
      .map(r => (r._1, (r._2,r._4)))
      .leftOuterJoin(global_avg_pred).map { case (item, ((user,usr_avg), global_avg_dev)) =>
      (user,item,usr_avg ,global_avg_dev.getOrElse(-999.0))}

    val prediction_2=global_avg_rdd
      .map(r=>(if (r._4 != -999.0) ((r._1 ,r._2), (r._3+r._4 * scale(r._3+r._4,r._3)))  else ((r._1 ,r._2),avg_train) ))

    prediction_2

    /* ********** ASSERT Block, Commented for faster processing during timing ********************
 // check that ratings were normalized and are within range [-1,1]
   assert (normalized_deviation.filter(r=> (r.rating > 1.0) || (r.rating < - 1.0)).count == 0)
 // check that final baseline prediction is comprised in the range of [-1,1]
   assert (prediction_2.filter(r=>( (r._2 < 1.0) || (r._2 > 5.0))).count == 0.0)
 // check unique combination of item and user --> user doesn't rate more than 1 time a movie
 //grouping by items leads to unique set of users on wich we average
   assert( normalized_deviation.map(x => (x.user,x.item)).distinct.count == train.count )
 // check that RDD after joining operation still contains all items
   assert ( normalized_deviation_prep.map(r=>r._1).distinct.count == train.map(r=>r.item).distinct.count)
//check that RDD size is unchanged after join operation
   assert( normalized_deviation_prep.count == train.count)
 // check that leftouterjoin went well and missing items were replace by -999 value
 // check that the sum of missing items and present items equals the total number of items
   val joined_items = global_avg_rdd.filter(x=>x._4 != -999.0).count
   val unjoined_items=global_avg_rdd.filter(x=>x._4 == -999.0).count
   assert ( joined_items+unjoined_items == test.count)
 */

  }

  // Compute mae for bonus case
  val testbonus=Predictor.baseline_predictor(train,test,true)

  val maefc = mae(test.map(r => ((r.user, r.item),r.rating)).join(testbonus)
    .map { case ((user,item), (rating, pred)) => (user,item,rating , pred )})
  println( "mae bonus " + maefc)

// time baseline prediction
    val time_baseline_avg_pred = Timer.timer(Predictor.baseline_predictor(train,test).collect())

// compute baseline prediction
    val baseline_prediction=Predictor.baseline_predictor(train,test)

    val mae_prediction_baseline=mae(test.map(r => ((r.user, r.item),r.rating)).join(baseline_prediction)
      .map { case ((user,item), (rating, pred)) => (user,item,rating , pred )})

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
              "Q3.1.4" -> Map(
                "MaeGlobalMethod" -> mae_global_avg, // Datatype of answer: Double
                "MaePerUserMethod" -> mae_per_user, // Datatype of answer: Double
                "MaePerItemMethod" -> mae_per_item, // Datatype of answer: Double
                "MaeBaselineMethod" ->  mae_prediction_baseline// Datatype of answer: Double


              ),
             "Q3.1.5" -> Map(
                "DurationInMicrosecForGlobalMethod" -> Map(
                  "min" ->time_global_avg_pred.min_times,  // Datatype of answer: Double
                  "max" -> time_global_avg_pred.max_times,  // Datatype of answer: Double
                  "average" -> time_global_avg_pred.avg_times, // Datatype of answer: Double
                  "stddev" -> time_global_avg_pred.stddev_times // Datatype of answer: Double
                ),
                "DurationInMicrosecForPerUserMethod" -> Map(
                  "min" -> time_user_avg_pred.min_times,  // Datatype of answer: Double
                  "max" -> time_user_avg_pred.max_times,  // Datatype of answer: Double
                  "average" ->time_user_avg_pred.avg_times, // Datatype of answer: Double
                  "stddev" -> time_user_avg_pred.stddev_times // Datatype of answer: Double
                ),
          "DurationInMicrosecForPerItemMethod" -> Map(
                  "min" -> time_item_avg_pred.min_times,  // Datatype of answer: Double
                  "max" -> time_item_avg_pred.max_times,  // Datatype of answer: Double
                  "average" ->time_item_avg_pred.avg_times, // Datatype of answer: Double
                  "stddev" -> time_item_avg_pred.stddev_times // Datatype of answer: Double

                ),
                "DurationInMicrosecForBaselineMethod" -> Map(
                  "min" -> time_baseline_avg_pred.min_times,  // Datatype of answer: Double
                  "max" -> time_baseline_avg_pred.max_times, // Datatype of answer: Double
                  "average" -> time_baseline_avg_pred.avg_times, // Datatype of answer: Double
                  "stddev" -> time_baseline_avg_pred.stddev_times // Datatype of answer: Double
                ),
                "RatioBetweenBaselineMethodAndGlobalMethod" ->time_baseline_avg_pred.avg_times/time_global_avg_pred.avg_times // Datatype of answer: Double
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
