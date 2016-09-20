/**
  * Created by vasu on 9/17/16.
  */
package Vijayakumar_Vasuprada_tweets
import org.apache.spark
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import java.io._
import org.apache.commons.lang.StringEscapeUtils
import scala.io.Source
import scala.util.parsing.json._
import org.apache.spark.{SparkConf, SparkContext}

object TFDF {
  //val tweet_cleaned = new ListBuffer[String]()
  //val score_map = scala.collection.mutable.Map[String, Int]()

  var tweet_id = 0 //Output has key as Line_No

  def compute(tweet: String): Array[(String,(Int,Int))] = {
    val occurence_map = scala.collection.mutable.Map[String, Int]()
    //val regex = "((.*[@|#].*)|([^0-9A-Za-z-])|(\\w+:\\/\\/\\S*))".r
    val regex = "((RT[ |a-z|A-Z|0-9]*)|([@|#].*)|(\\w+:\\/\\/\\S*)|(\\w+([.|@]+\\w+)+))".r
    val json_string = JSON.parseFull(tweet)
    val tweet_text = json_string.get.asInstanceOf[Map[String, Any]]
    val tweet_clean = tweet_text("text").toString.split("\\s+")
    //println("The tweet is:", tweet_text.get("text"))
    tweet_id += 1
    var str = ""
    for (remove_chars <- tweet_clean)
    {
      if (regex.replaceFirstIn(remove_chars, "") != "") {
        str += remove_chars.replaceAll("[^a-zA-Z0-9'’]+","").toLowerCase() + " "
      }
    }
    val tweet_list = str.split(" ")
    for(item <- tweet_list)
        if(!occurence_map.contains(item))
            occurence_map(item) = tweet_list.count(_ == item)

    str.split(" ").filter(a=> !a.equals("")).distinct.map(a=> {(StringEscapeUtils.escapeJava(a),(tweet_id,occurence_map(a)))})
  }


  def main(args: Array[String]) {

    val sparkconf = new SparkConf().setAppName("TFDF_Score")
    val sc = new SparkContext(sparkconf)
    /*val fileLines = Source.fromFile("/home/vasu/HW1_INF553/AFINN-111.txt").getLines.toList
    for (line <- fileLines) {
      val line_item = line.split("\\s")
      val score = line_item.last
      val init = line_item.view.init
      score_map(init.mkString(" ")) = score.toInt
    }*/
    val text = sc.textFile(args(0))

    val result2 = text.flatMap(compute).groupByKey().reduceByKey((a,b) => b).sortByKey(true).collect()
    val list = new ListBuffer[String]()

    for(tuple <- result2){
      val pair_list = new ListBuffer[(Int,Int)]()
        for(pair <- tuple._2) {
          pair_list += pair
        }
        list += "(" + tuple._1 + "," +  tuple._2.size + "," + pair_list.toList + ")"

    }
    sc.parallelize(list.toList).saveAsTextFile("Vijayakumar_Vasuprada_tweets_tfdf_first20")

  }

}
