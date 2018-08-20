package org.scala.learning.cousera

object PatternMatch {
  def test(n: Int): String = n%2 match{
    case 0 => "Even"
    case _ => "Odd"
  }
  def main(args: Array[ String ]): Unit={
    println( test(1) )
    println( test(2) )
    println( test(3) )
  }
}