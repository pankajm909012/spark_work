package org.spark.ipl.df

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object MatchResultUsingDataFrame {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
    conf.set("spark.master", "local")
    conf.set("spark.app.name", "IPL-spark-application-DataFrame")
    val sc = new SparkContext(conf)
    val sqc = new SQLContext(sc)
    import sqc.implicits._
    sc.setLogLevel("WARN")
    val rdd = sc.textFile("/ipl/deliveries.csv")
    val delRdd = rdd.subtract(sc.parallelize(rdd.take(1)))
    val delDF = delRdd.map(line => {
    	val str = line.split(",")
    			var wkt = 0
      
      if (str.length > 18 && !str(18).trim().isEmpty())
        wkt = 1;

      (str(0).toInt, str(1), str(2), str(3), str(4).toInt, str(17).toInt, wkt)

    }).toDF("match_id", "inning", "batting_team", "bowling_team", "over", "run", "player_dismissed")
    
    /*
      +--------+------+-------------------+--------------------+----+---+----------------+
      |match_id|inning|       batting_team|        bowling_team|over|run|player_dismissed|
      +--------+------+-------------------+--------------------+----+---+----------------+
      |       1|     1|Sunrisers Hyderabad|Royal Challengers...|   1|  0|               0|
      |       1|     1|Sunrisers Hyderabad|Royal Challengers...|   1|  0|               0|
      |       1|     1|Sunrisers Hyderabad|Royal Challengers...|   1|  4|               0|
      |       1|     1|Sunrisers Hyderabad|Royal Challengers...|   1|  0|               0|
      |       1|     1|Sunrisers Hyderabad|Royal Challengers...|   1|  2|               0|
      |       1|     1|Sunrisers Hyderabad|Royal Challengers...|   1|  0|               0|
      |       1|     1|Sunrisers Hyderabad|Royal Challengers...|   1|  1|               0|
      |       1|     1|Sunrisers Hyderabad|Royal Challengers...|   2|  1|               0|
      |       1|     1|Sunrisers Hyderabad|Royal Challengers...|   2|  4|               0|
      |       1|     1|Sunrisers Hyderabad|Royal Challengers...|   2|  1|               0|
     	+--------+------+-------------------+--------------------+----+---+----------------+
     */
    val playingTeamDF = delDF.dropDuplicates(Array("match_id", "inning")).select("match_id", "inning", "batting_team").toDF("match_id", "inning", "team")
    /*
    +--------+------+--------------------+
    |match_id|inning|                team|
    +--------+------+--------------------+
    |     102|     2|     Deccan Chargers|
    |      31|     1|    Delhi Daredevils|
    |     385|     1|    Rajasthan Royals|
    |     391|     2| Chennai Super Kings|
    |     547|     1| Chennai Super Kings|
    |     553|     2|    Delhi Daredevils|
    |      25|     1| Sunrisers Hyderabad|
    |     261|     1|Kochi Tuskers Kerala|
    |      31|     2|Kolkata Knight Ri...|
    |     379|     1| Chennai Super Kings|
    |     385|     2|    Delhi Daredevils|
    |     423|     1|      Mumbai Indians|
    +--------+------+--------------------+
    */
    val battingTeam = playingTeamDF.where($"inning" === "1").select("match_id", "team")
    /*
    +--------+--------------------+
    |match_id|                team|
    +--------+--------------------+
    |      31|    Delhi Daredevils|
    |     385|    Rajasthan Royals|
    |     547| Chennai Super Kings|
    |      25| Sunrisers Hyderabad|
    |     261|Kochi Tuskers Kerala|
    |     379| Chennai Super Kings|
    |     423|      Mumbai Indians|
    |      19| Sunrisers Hyderabad|
    |     255|Kochi Tuskers Kerala|
    |     417|      Mumbai Indians|
    |     131| Chennai Super Kings|
    |     249|     Deccan Chargers|
    |     582|Rising Pune Super...|
    |     125|    Delhi Daredevils|
    +--------+--------------------+
      */
    val bowlingTeam = playingTeamDF.where($"inning" === "2").select("match_id", "team")
    /*
    +--------+--------------------+
    |match_id|                team|
    +--------+--------------------+
    |     102|     Deccan Chargers|
    |     391| Chennai Super Kings|
    |     553|    Delhi Daredevils|
    |      31|Kolkata Knight Ri...|
    |     385|    Delhi Daredevils|
    |     547|Kolkata Knight Ri...|
    |      25|Rising Pune Super...|
    |     261|    Rajasthan Royals|
    |     379|      Mumbai Indians|
    |     423| Sunrisers Hyderabad|
    |      19|     Kings XI Punjab|
    |     255|Kolkata Knight Ri...|
    |     417|Royal Challengers...|
    +--------+--------------------+
      */
    val inningRunDF = delDF.groupBy("match_id", "inning").agg(sum("run"), max("over"), sum("player_dismissed")).toDF("match_id", "inning", "total_run", "over", "wicket")
    /*
    +--------+------+---------+----+------+
    |match_id|inning|total_run|over|wicket|
    +--------+------+---------+----+------+
    |      31|     1|      160|  20|     6|
    |     102|     2|      153|  20|     7|
    |     385|     1|      165|  20|     7|
    |     391|     2|      139|  18|     0|
    |     547|     1|      134|  20|     6|
    |     553|     2|      175|  20|     7|
    |      25|     1|      176|  20|     3|
    |      31|     2|      161|  17|     3|
    |     261|     1|      109|  20|    10|
    |     379|     1|      187|  20|     5|
    |     385|     2|      160|  20|     6|
    |     423|     1|      129|  20|     4|
    |     547|     2|      132|  20|     9|
    |      19|     1|      159|  20|     6|
    +--------+------+---------+----+------+
     */
    val firstInningRunDF = inningRunDF.where($"inning" === "1").select("match_id", "total_run", "wicket", "over");
    /*
    +--------+---------+------+----+
    |match_id|total_run|wicket|over|
    +--------+---------+------+----+
    |      31|      160|     6|  20|
    |     385|      165|     7|  20|
    |     547|      134|     6|  20|
    |      25|      176|     3|  20|
    |     261|      109|    10|  20|
    |     379|      187|     5|  20|
    |     423|      129|     4|  20|
    |      19|      159|     6|  20|
    |     255|      132|     7|  20|
    |     417|      194|     7|  20|
    |     131|      165|     6|  20|
    |     249|      165|     8|  20|
    +--------+---------+------+----+
     */
    val secondInningRunDF = inningRunDF.where($"inning" === "2").select("match_id", "total_run", "wicket", "over");
    /*
    +--------+---------+------+----+
    |match_id|total_run|wicket|over|
    +--------+---------+------+----+
    |     102|      153|     7|  20|
    |     391|      139|     0|  18|
    |     553|      175|     7|  20|
    |      31|      161|     3|  17|
    |     385|      160|     6|  20|
    |     547|      132|     9|  20|
    |      25|      179|     4|  20|
    |     261|      111|     2|  15|
    |     379|      149|     9|  20|
    |     423|      130|     3|  18|
    |      19|      154|    10|  20|
    |     255|      126|     9|  20|
    |     417|      136|     7|  20|
    +--------+---------+------+----+ 
     */
    val fristInning = firstInningRunDF.join(battingTeam, Seq("match_id")).toDF("match_id", "1_total_run", "1_wicket", "1_over", "bating")
    /*
    +--------+-----------+--------+------+--------------------+
    |match_id|1_total_run|1_wicket|1_over|              bating|
    +--------+-----------+--------+------+--------------------+
    |      31|        160|       6|    20|    Delhi Daredevils|
    |     385|        165|       7|    20|    Rajasthan Royals|
    |     547|        134|       6|    20| Chennai Super Kings|
    |      25|        176|       3|    20| Sunrisers Hyderabad|
    |     261|        109|      10|    20|Kochi Tuskers Kerala|
    |     379|        187|       5|    20| Chennai Super Kings|
    |     423|        129|       4|    20|      Mumbai Indians|
    |      19|        159|       6|    20| Sunrisers Hyderabad|
    |     255|        132|       7|    20|Kochi Tuskers Kerala|
    |     417|        194|       7|    20|      Mumbai Indians|
    |     131|        165|       6|    20| Chennai Super Kings|
    |     249|        165|       8|    20|     Deccan Chargers|
    |     582|        163|       5|    20|Rising Pune Super...|
    +--------+-----------+--------+------+--------------------+
     */
    val secondInning = secondInningRunDF.join(bowlingTeam, Seq("match_id")).toDF("match_id", "2_total_run", "2_wicket", "2_over", "bowling")
    /*
    +--------+-----------+--------+------+--------------------+
    |match_id|2_total_run|2_wicket|2_over|             bowling|
    +--------+-----------+--------+------+--------------------+
    |     102|        153|       7|    20|     Deccan Chargers|
    |     391|        139|       0|    18| Chennai Super Kings|
    |     553|        175|       7|    20|    Delhi Daredevils|
    |      31|        161|       3|    17|Kolkata Knight Ri...|
    |     385|        160|       6|    20|    Delhi Daredevils|
    |     547|        132|       9|    20|Kolkata Knight Ri...|
    |      25|        179|       4|    20|Rising Pune Super...|
    |     261|        111|       2|    15|    Rajasthan Royals|
    |     379|        149|       9|    20|      Mumbai Indians|
    |     423|        130|       3|    18| Sunrisers Hyderabad|
    |      19|        154|      10|    20|     Kings XI Punjab|
    |     255|        126|       9|    20|Kolkata Knight Ri...|
    |     417|        136|       7|    20|Royal Challengers...|
    |     131|        169|       4|    20|     Deccan Chargers|
    +--------+-----------+--------+------+--------------------+
     
     */
    val matchDF = fristInning.join(secondInning, Seq("match_id"))
    /*
    +--------+-----------+--------+------+--------------------+-----------+--------+------+--------------------+
    |match_id|1_total_run|1_wicket|1_over|              bating|2_total_run|2_wicket|2_over|             bowling|
    +--------+-----------+--------+------+--------------------+-----------+--------+------+--------------------+
    |     286|        136|       8|    20|     Deccan Chargers|        137|       4|    19|       Pune Warriors|
    |     330|        124|       7|    20|     Kings XI Punjab|        127|       2|    17|Kolkata Knight Ri...|
    |     448|        190|       3|    20|Royal Challengers...|        194|       4|    18|     Kings XI Punjab|
    |     287|        163|       8|    20|     Kings XI Punjab|         87|      10|    13|      Mumbai Indians|
    |     331|        187|       4|    20|     Deccan Chargers|        193|       1|    17|    Delhi Daredevils|
    |     449|        136|       9|    20| Sunrisers Hyderabad|        113|       9|    20|    Rajasthan Royals|
    |     170|        165|       8|    20|      Mumbai Indians|        166|       6|    18|    Delhi Daredevils|
    |     288|        146|       6|    20|    Rajasthan Royals|        151|       1|    17|Royal Challengers...|
    |     332|        164|       5|    20| Chennai Super Kings|        151|       7|    20|       Pune Warriors|
    |     171|        170|       4|    20|Royal Challengers...|        158|       6|    20|     Deccan Chargers|
    |     289|        176|       4|    20| Chennai Super Kings|        158|       6|    20|    Delhi Daredevils|
    |     333|        163|       6|    20|     Kings XI Punjab|        166|       5|    20|Royal Challengers...|
    |     172|        153|       8|    20|    Delhi Daredevils|        154|       4|    18|     Deccan Chargers|
    +--------+-----------+--------+------+--------------------+-----------+--------+------+--------------------+
     
     */
    val matchResult = matchDF.map(row => {
      val matchId = row(0).toString()
      val firstInningRun = row(1).toString().toInt
      val secondInningRun = row(5).toString().toInt
      val bating = row(4).toString()
      val bowling = row(8).toString()
      var winner = ""
      var loser = ""
      var winBy = ""
      if (firstInningRun > secondInningRun) {
        val runs = firstInningRun - secondInningRun
        winBy = runs + " runs"
        winner = bating
        loser = bowling
      } else {
        val wickets = 10 - row(6).toString().toInt
        winBy = wickets + " wickets"
        winner = bowling
        loser = bating
      }
      (matchId, winner, loser, winBy)
    }).toDF("match_id", "winner", "loser", "winby")
    
    /*
    +--------+--------------------+--------------------+---------+
    |match_id|              winner|               loser|    winby|
    +--------+--------------------+--------------------+---------+
    |     286|       Pune Warriors|     Deccan Chargers|6 wickets|
    |     330|Kolkata Knight Ri...|     Kings XI Punjab|8 wickets|
    |     448|     Kings XI Punjab|Royal Challengers...|6 wickets|
    |     287|     Kings XI Punjab|      Mumbai Indians|  76 runs|
    |     331|    Delhi Daredevils|     Deccan Chargers|9 wickets|
    |     449| Sunrisers Hyderabad|    Rajasthan Royals|  23 runs|
    |     170|    Delhi Daredevils|      Mumbai Indians|4 wickets|
    |     288|Royal Challengers...|    Rajasthan Royals|9 wickets|
    |     332| Chennai Super Kings|       Pune Warriors|  13 runs|
    |     171|Royal Challengers...|     Deccan Chargers|  12 runs|
    |     289| Chennai Super Kings|    Delhi Daredevils|  18 runs|
    |     333|Royal Challengers...|     Kings XI Punjab|5 wickets|
    +--------+--------------------+--------------------+---------+
     
     */
    val matchAnalysis = matchDF.join(matchResult, Seq("match_id")).sort("match_id")
    /*
		+--------+-----------+--------+------+--------------------+-----------+--------+------+--------------------+--------------------+--------------------+----------+
    |match_id|1_total_run|1_wicket|1_over|              bating|2_total_run|2_wicket|2_over|             bowling|              winner|               loser|     winby|
    +--------+-----------+--------+------+--------------------+-----------+--------+------+--------------------+--------------------+--------------------+----------+
    |       1|        207|       4|    20| Sunrisers Hyderabad|        172|      10|    20|Royal Challengers...| Sunrisers Hyderabad|Royal Challengers...|   35 runs|
    |       2|        184|       8|    20|      Mumbai Indians|        187|       3|    20|Rising Pune Super...|Rising Pune Super...|      Mumbai Indians| 7 wickets|
    |       3|        183|       4|    20|       Gujarat Lions|        184|       0|    15|Kolkata Knight Ri...|Kolkata Knight Ri...|       Gujarat Lions|10 wickets|
    |       4|        163|       6|    20|Rising Pune Super...|        164|       4|    19|     Kings XI Punjab|     Kings XI Punjab|Rising Pune Super...| 6 wickets|
    |       5|        157|       8|    20|Royal Challengers...|        142|       9|    20|    Delhi Daredevils|Royal Challengers...|    Delhi Daredevils|   15 runs|
    |       6|        135|       7|    20|       Gujarat Lions|        140|       1|    16| Sunrisers Hyderabad| Sunrisers Hyderabad|       Gujarat Lions| 9 wickets|
    |       7|        178|       7|    20|Kolkata Knight Ri...|        180|       6|    20|      Mumbai Indians|      Mumbai Indians|Kolkata Knight Ri...| 4 wickets|
    |       8|        148|       4|    20|Royal Challengers...|        150|       2|    15|     Kings XI Punjab|     Kings XI Punjab|Royal Challengers...| 8 wickets|
    |       9|        205|       4|    20|    Delhi Daredevils|        108|      10|    17|Rising Pune Super...|    Delhi Daredevils|Rising Pune Super...|   97 runs|
    |      10|        158|       8|    20| Sunrisers Hyderabad|        159|       6|    19|      Mumbai Indians|      Mumbai Indians| Sunrisers Hyderabad| 4 wickets|
    |      11|        170|       9|    20|     Kings XI Punjab|        171|       2|    17|Kolkata Knight Ri...|Kolkata Knight Ri...|     Kings XI Punjab| 8 wickets|
    |      12|        142|       5|    20|Royal Challengers...|        145|       6|    19|      Mumbai Indians|      Mumbai Indians|Royal Challengers...| 4 wickets|
    |      13|        171|       8|    20|Rising Pune Super...|        172|       3|    18|       Gujarat Lions|       Gujarat Lions|Rising Pune Super...| 7 wickets|
		+--------+-----------+--------+------+--------------------+-----------+--------+------+--------------------+--------------------+--------------------+----------+
     * */
    matchAnalysis.coalesce(1).rdd.saveAsTextFile("/results/ipl/matchresults/df") //using coalesce for store results into single file
  
  }
}