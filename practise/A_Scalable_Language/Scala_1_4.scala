package org.scala.learning.A_Scalable_Language

class MyClass( index : Int, name : String){
  override def toString(): String={
		 "Index: " + this.index + " Name: " + this.name
  }
}
object Scala_1_4 {
  def main( args : Array[ String ] ) : Unit = {
     val a1 = new MyClass( 10, "Pankaj")
     val a2 = new MyClass( 20, "Ranjit")
     println(a1)
     println(a2)
  }
}