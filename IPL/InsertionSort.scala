package org.scala.learning.cousera

object InsertionSort {
  def isort(xs: List[Int]): List[Int] = xs match {
    case List()  => List()
    case y :: ys => insert(y,isort(ys))
  }
  def insert(x: Int, xs: List[Int]): List[Int] = xs match {
    case List()  => List(x)
    case y :: ys => if (x <= y) x :: xs else y :: insert(x, ys)
  }
  
  def main(args:Array[String]):Unit={
    val arr:List[Int]=List(2,5,4,9,8,5)
    println(arr)
    println(isort(arr))
  }
}