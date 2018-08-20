package org.scala.learning.A_Scalable_Language

object Scala_1_5 {
  def main( args : Array[ String ] ) : Unit ={
    val name="Pankaj"
    val behaviour = "good"
    val nameHasUpperCase = name.exists( _.isUpper ) //shorthand
    val nameHasUpperCase1 = name.exists{ x => x.isUpper } //proper lambda
    println(nameHasUpperCase + ", " + nameHasUpperCase1)
    val behaviourHasUpperCase = behaviour.exists( _.isUpper ) // shorthand
    val behaviourHasUpperCase1 = behaviour.exists { x => x.isUpper }// proper lambda
    println(behaviourHasUpperCase + ", " + behaviourHasUpperCase1)
  }
}