package org.spark.ipl

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MatchResult {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.set("spark.master", "local")
    conf.set("spark.app.name", "IPL-spark-application")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    var rdd = sc.textFile("/ipl/deliveries.csv")
    val delRdd = rdd.subtract(sc.parallelize(rdd.take(1))).map(line => line.split(","))
    val teamMatchRdd = delRdd.map(line => (line(0) + line(1), line(2))).reduceByKey((a, b) => a)
    val wktRdd = delRdd.map(line => {
      val inningId = line(0) + line(1)
      if (line.length > 18 && !line(18).trim().isEmpty())
        (inningId, "1")
      else
        (inningId, "0")
    }).reduceByKey((a, b) => (a.toInt + b.toInt).toString)

    val scoreRdd = delRdd.map(line => (line(0) + line(1), line(17))).reduceByKey((a, b) => (a.toInt + b.toInt).toString)
    val matchScoreRdd = teamMatchRdd.join(scoreRdd.join(wktRdd))
    val matchResultRdd = matchScoreRdd.map(line => {
      val toss = line._1.substring(line._1.length() - 1, line._1.length())
      val matchId = line._1.substring(0, line._1.length() - 1)
      if (toss.trim().equals("1"))
        (matchId, ("B", line._2._1, line._2._2))
      else
        (matchId, ("F", line._2._1, line._2._2))
    }).groupByKey().map(line => {
        val team1 = line._2.head
        val team2 = line._2.last
        var winBy=""
        if (team1._3._1.toInt > team2._3._1.toInt){
          if(team1._1.equals("B"))
              winBy=(team1._3._1.toInt - team2._3._1.toInt).toString+" runs"
          else
              winBy=(10 - team1._3._2.toInt).toString+" wickets"
              
          (line._1.toInt,"Winner:" + team1._2+"("+team1._3._1.toInt+"/"+team1._3._2+")" + " Losser:" + team2._2++"("+team2._3._1.toInt+"/"+team2._3._2+")"+" Win by:"+winBy)
        }else{
          if(team2._1.equals("B"))
              winBy=(team2._3._1.toInt - team1._3._1.toInt).toString+" runs"
          else
              winBy=(10 - team2._3._2.toInt).toString+" wickets"
          (line._1.toInt,"Winner:" + team2._2 +"("+team2._3._1.toInt+"/"+team2._3._2+")" + " Losser:" + team1._2+"("+team1._3._1.toInt+"/"+team1._3._2+")" +" Win by:"+winBy)
        }
      }).sortByKey()
      matchResultRdd.saveAsTextFile("/results/ipl/matchresults")
    sc.stop()
  }
  
}