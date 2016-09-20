
/**
  * Created by vasu on 9/13/16.
  */
package Vijayakumar_Vasuprada_tweets
import org.apache.spark
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer

import scala.io.Source
import scala.util.parsing.json._
import org.apache.spark.{SparkConf, SparkContext}

object Sentiment {
  //val tweet_cleaned = new ListBuffer[String]()
  val score_map = scala.collection.mutable.Map[String, Int]()
  var tweet_id = 0 //Output has key as Line_No

  def compute(tweet: String): Array[(Int,Int)] = {
    //val regex = "((.*[@|#].*)|([^0-9A-Za-z-])|(\\w+:\\/\\/\\S*))".r
    val regex = "((RT[ |a-z|A-Z|0-9]*)|([@|#].*)|(\\w+:\\/\\/\\S*)|(\\w+([.|@]+\\w+)+))".r
    val json_string = JSON.parseFull(tweet)
    val tweet_text = json_string.get.asInstanceOf[Map[String, Any]]
    val tweet_clean = tweet_text("text").toString.split("\\s+")
    tweet_id += 1
    var str = ""
    for (remove_chars <- tweet_clean)
      {
        if (regex.replaceFirstIn(remove_chars, "") != "") {
            str += remove_chars.replaceAll("[^a-zA-Z0-9'’]+","").toLowerCase() + " "
        }
      }

    str.split(" ").filter(a => !a.equals("")).map(a=> {
        if (score_map.contains(a)) //Creating the Tuples(LineNo,Sentiment Score)
          (tweet_id, score_map(a))
        else
          (tweet_id, 0) //If not found then sentiment score is 0
    })
  }

    def main(args: Array[String]) {

      val sparkconf = new SparkConf().setAppName("Sentiment_Score")
      val sc = new SparkContext(sparkconf)
      val fileLines = Source.fromFile(args(1)).getLines.toList
      for (line <- fileLines) {
        val line_item = line.split("\\s")
        val score = line_item.last
        val init = line_item.view.init
        score_map(init.mkString(" ")) = score.toInt
      }

      val text = sc.textFile(args(0))
      val result = text.flatMap(compute).reduceByKey((a, b) => a + b).sortByKey(true)


      result.saveAsTextFile("Vijayakumar_Vasuprada_tweets_sentiment_first20")
    }
  }




