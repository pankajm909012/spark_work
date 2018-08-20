package org.scala.learning.A_Scalable_Language


object Scala_1_2 {
  def factorial( x: BigInt ): BigInt = {
    if( x == 0 )return 1 else x*factorial( x - 1 )
  }
  def main( args : Array[ String ] ): Unit ={
    print( factorial( 30 ) )
  }
}