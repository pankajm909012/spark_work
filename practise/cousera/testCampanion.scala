package org.scala.learning.cousera


object testCampanion {
  def main(args:Array[String]):Unit={
    val emp1 = Employee(10,"Pankaj",30897654)
		val emp2 = Employee(20,"Ranjit",6456789)
		val emp3 = Employee(30,"Sanjay",5000000)
		val emp4 = Employee(40,"Reshmi",4000000)
		val lt = List(emp1,emp2,emp3,emp4)
		lt.foreach(println)
  }
}