package org.spark.ipl.df

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.ScalaReflection
import junit.framework.TestCase
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.types.IntegerType

object Inning extends Enumeration {
  type Inning = Value
  val BATTING, BOWLING, SUPER_BATTING, SUPER_BOWLING = Value
}
import Inning._
object MatchResultDataFrame {

  val newMatch = Seq("matchId", "battingTeam", "battingRun", "battingWicket", "battingOver", "bowlingTeam", "bowlingRun", "bowlingWicket", "bowlingOver", "winner", "method", "winBy")
  val newSuper = Seq("matchId", "super_winner", "super_winBy")
  val sc = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("IPL-spark-application-DataFrame")
    new SparkContext(conf)
  }
  val sqc = {
    new SQLContext(sc)
  }
  import sqc.implicits._
  def schema={
    val schemaRDD = StructType(
     List(
       StructField("matchId",IntegerType,true),
       StructField("battingTeam",StringType,true),
       StructField("battingRun",StringType,true),
       StructField("battingWicket",StringType,true),
       StructField("battingOver",StringType,true),
       StructField("bowlingTeam",StringType,true),
       StructField("bowlingRun",StringType,true),
       StructField("bowlingWicket",StringType,true),
       StructField("bowlingOver",StringType,true),
       StructField("winner",StringType,true),
       StructField("method",StringType,true),
       StructField("winBy",StringType,true)
       )
     )
     schemaRDD
  }
  def creatRdd(filePath: String) = {
    val rdd = sc.textFile(filePath)
    rdd.subtract(sc.parallelize(rdd.take(1)))
  }
  def delivery(rdd: RDD[String]) = {
    rdd.map(line => {
      val str = line.split(",")
      val matchId = str(0).toInt
      val inning = Inning(str(1).toInt - 1).toString()
      val battingTeam = str(2)
      val bowlingTeam = str(3)
      val over = str(4).toInt
      val run = str(17).toInt
      var wkt = 0
      if (str.length > 18 && !str(18).trim().isEmpty())
        wkt = 1
      (matchId, inning, battingTeam, bowlingTeam, over, run, wkt)
    }).toDF("matchId", "inning", "battingTeam", "bowlingTeam", "over", "run", "wicket")
  }
  def iplMatch(rdd: RDD[String]) = {
    rdd.map(line => {
      val str = line.split(",")
      val matchId = str(0).toInt
      var winner = str(10)
      if (winner.isEmpty())
        winner = "no result"
      val winRun = str(11).toInt
      val winWkt = str(12).toInt
      var winBy = ""
      if (winRun == 0)
        winBy = winWkt + " wicket"
      else if (winWkt == 0)
        winBy = winRun + " runs"
      if (str(9).toInt == 1)
        winBy += " by DL"
      (matchId, str(8), str(9).toInt, winner, winBy)
    }).toDF("matchId", "result", "DL", "dl_winner", "dl_winBy")
  }
  def team(delivery: DataFrame, inning: Inning) = {
    delivery
      .where($"inning" === inning.toString())
      .groupBy("matchId", "inning", "battingTeam")
      .agg(sum("run"), max("over"), sum("wicket"))
      .select("matchId", "battingTeam", "sum(run)", "sum(wicket)", "max(over)").toDF("matchId", inning.toString() + "_name", inning.toString() + "_run", inning.toString() + "_wicket", inning.toString() + "_over")
  }
  def result(iplmatch: DataFrame, battingInning: Inning, bowlingInning: Inning, method: String) = {
    iplmatch.map(row => {
      val matchId = row.getAs[Int]("matchId")
      val firstInningRun = row.getAs[Long](battingInning.toString() + "_run").toInt
      val secondInningRun = row.getAs[Long](bowlingInning.toString() + "_run").toInt
      val bating = row.getAs[String](battingInning.toString() + "_name")
      val bowling = row.getAs[String](bowlingInning.toString() + "_name")
      var winner = ""
      var loser = ""
      var winBy = ""
      if (firstInningRun > secondInningRun) {
        val runs = firstInningRun - secondInningRun
        winBy = runs + " runs"
        winner = bating
        loser = bowling
      } else if (firstInningRun < secondInningRun) {
        val wickets = 10 - row.getAs[Long](bowlingInning.toString() + "_wicket")
        winBy = wickets + " wickets"
        winner = bowling
        loser = bating
      }
      val battingWicket = row.getAs[Long](battingInning.toString() + "_wicket").toInt
      val battingOver = row.getAs[Int](battingInning.toString() + "_over")
      val bowlingWicket = row.getAs[Long](bowlingInning.toString() + "_wicket").toInt
      val bowlingOver = row.getAs[Int](bowlingInning.toString() + "_over")
      (matchId, bating, firstInningRun, battingWicket, battingOver, bowling, secondInningRun, bowlingWicket, bowlingOver, winner, method, winBy)
    }).toDF(newMatch: _*)
  }

  def analysisResult(properMatchDF: DataFrame, superOverMatchResult: DataFrame, improperMatchDF: DataFrame) = {
    val matchDF = properMatchDF.join(superOverMatchResult, Seq("matchId"), "left").join(improperMatchDF, Seq("matchId"), "left")
    matchDF.rdd.map(row => {
      val matchId = row.getAs[Int]("matchId")
      
      val battingTeam = row.getAs[String]("battingTeam")
      val battingRun = row.getAs[Int]("battingRun").toString()
      val battingWicket = row.getAs[Int]("battingWicket").toString()
      val battingOver = row.getAs[Int]("battingOver").toString()

      val bowlingTeam = row.getAs[String]("bowlingTeam")
      val bowlingRun = row.getAs[Int]("bowlingRun").toString()
      val bowlingWicket = row.getAs[Int]("bowlingWicket").toString()
      val bowlingOver = row.getAs[Int]("bowlingOver").toString()
      
      var winBy = row.getAs[String]("winBy")
      var method = row.getAs[String]("method")
      var winner = row.getAs[String]("winner")
      
      val superOverWinner=row.getAs[String]("super_winner")
      val superOverWinBy=row.getAs[String]("super_winBy")
      if(superOverWinner!=null && superOverWinBy!=null){
        winBy=superOverWinBy
        method="Super Over"
        winner=superOverWinner
      }
      val dlWinner=row.getAs[String]("dl_winner")
      val dlWinBy = row.getAs[String]("dl_winBy")
      if(dlWinner!=null && dlWinBy!=null){
        winBy=dlWinBy
        method="DL"
        winner=dlWinner
      }
       
      Row(matchId, battingTeam, battingRun, battingWicket, battingOver, bowlingTeam, bowlingRun, bowlingWicket, bowlingOver, winner, method, winBy)
    })
  }
  def parseSuperOverResult(superMatch: DataFrame) = {
    superMatch.map(row => {
      val matchId = row.getAs[Int]("matchId")

      val battingTeam = row.getAs[String]("battingTeam")
      val battingRun = row.getAs[Int]("battingRun")
      val battingWicket = row.getAs[Int]("battingWicket")
      val battingOver = row.getAs[Int]("battingOver")

      val bowlingTeam = row.getAs[String]("bowlingTeam")
      val bowlingRun = row.getAs[Int]("bowlingRun")
      val bowlingWicket = row.getAs[Int]("bowlingWicket")
      val bowlingOver = row.getAs[Int]("bowlingOver")
      val winner = row.getAs[String]("winner")
      val winBy = row.getAs[String]("winBy")

      val batting = "Batting: " + abbreviation(battingTeam) + " (" + battingRun + "/" + battingWicket + ", " + battingOver + ")"
      val bowling = "Bowling: " + abbreviation(bowlingTeam) + " (" + bowlingRun + "/" + bowlingWicket + ", " + bowlingOver + ")"
      (matchId, winner, batting + " " + bowling + " win: " + winBy)

    }).toDF(newSuper:_*)
  }
  def abbreviation(str: String) = {
    var abbre = ""
    str.split(" ").foreach { word =>
      abbre += word(0).toString()
    }
    abbre.toUpperCase()
  }
  def saveAsCSV(rdd:RDD[Row],schema:StructType)={
    val currentTime=System.currentTimeMillis().toString()
    val hdfsPath="/results/ipl/matchresults/df/"+currentTime
    val resultDF = sqc.createDataFrame(rdd,schema).sort("matchId")
    resultDF.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save(hdfsPath)
  }
  def iplAnalysis(filePath: Array[String]) = {
    import sqc.implicits._
    sc.setLogLevel("WARN")
    val delRdd = creatRdd(filePath(0))
    val mtRdd = creatRdd(filePath(1))
    
    val delDF = delivery(delRdd).persist
    val mtDF = iplMatch(mtRdd).persist
    
    val improperMatchDF = mtDF.where((col("DL") === "1") || ((col("result") !== "normal")&&(col("result") !== "tie")))
    
    val battingTeam = team(delDF, Inning.BATTING)
    val bowlingTeam = team(delDF, Inning.BOWLING)
    val matchPlayedDF = battingTeam.join(bowlingTeam, Seq("matchId")).persist()
    val properMatchResult = result(matchPlayedDF, Inning.BATTING, Inning.BOWLING, "Normal")
    
    val superOverBattingTeam = team(delDF, Inning.SUPER_BATTING)
    val superOverBowlingTeam = team(delDF, Inning.SUPER_BOWLING)
    val superOverMatchDF = superOverBattingTeam.join(superOverBowlingTeam, Seq("matchId"))
    val superOverMatchResult = result(superOverMatchDF, Inning.SUPER_BATTING, Inning.SUPER_BOWLING, "Super Over")
    val parsedSuperOverResult=parseSuperOverResult(superOverMatchResult)
    
    val iplMatchResult = analysisResult(properMatchResult, parsedSuperOverResult, improperMatchDF)
    iplMatchResult
  }
  def main(args: Array[String]): Unit = {
    saveAsCSV(iplAnalysis(Array("/ipl/deliveries.csv", "/ipl/matches.csv")),schema)
  }

}