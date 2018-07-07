package org.scala.learning.cousera

object mergeSort {
  def sort(xs: List[Int]): List[Int] = {
    val n = xs.length / 2
    if (n == 0) xs
    else {
      def merge(xs: List[Int], ys: List[Int]): List[Int] = {
        (xs,ys) match{
          case (Nil,ys)=> ys
          case (xs,Nil)=>xs
          case(x::xs1,y::ys1)=> if(x<y)x::merge(xs1,ys)else y:: merge(xs,ys1)
        }
        /*xs match {
          case Nil => ys
          case x :: xs1 => ys match {
            case Nil      => xs
            case y :: ys1 => if (x < y) x :: merge(xs1, ys) else y :: merge(xs, ys1)
          }
        }*/
      }
      val (fend, send) = xs splitAt n
      merge(sort(fend), sort(send))
    }
  }
  
  def main(args:Array[String]):Unit={
    val xs = List(9,-6,1,3,4,2,7,8)
    println(sort(xs))
  }
}