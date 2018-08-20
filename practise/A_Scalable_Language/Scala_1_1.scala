package org.scala.learning.A_Scalable_Language

object Scala_1_1 {
  def main(args: Array[String]): Unit = {
    var capital = Map("US" -> "Washington", "France" -> "Paris")
    println(capital)
    capital += ("Japan" -> "Tokyo")
    println(capital)
    println(capital("France"))
  }
}