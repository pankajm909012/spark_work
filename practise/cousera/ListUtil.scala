package org.scala.learning.cousera


object ListUtil {
 def last[T](xs : List[ T ]): T=xs match{
   case List() => throw new Error("Last on Empty lisl")
   case List(x) => x
   case y::ys => last(ys)
 }
 def init[T](xs : List[T]): List[T]=xs match{
   case List() => throw new Error("init on Empty list")
   case List(x)=> List()
   case y::ys => y::init(ys)
 }
 def concat[T](xs: List[T], ys: List[T]): List[T]=xs match{
   case List() => ys
   case z::zx => z::concat(zx, ys)
 }
 def revers[T](xs:List[T]): List[T]=xs match{
   case List() => List()
   case y::ys => revers(ys) ::: List(y)
 }
 def removeAt[T](n: Int,xs:List[T]):List[T]=(xs take n):::(xs drop n+1)
 
 def main(args: Array[String]):Unit ={
   val xs = List(2,5,4,3,9,7,8)
   val ys = List(10,60,30,20,70,40)
   println (init(xs))
   println (last(xs))
   println (concat(xs, ys))
   println(revers(xs))
   print (removeAt(3,xs))
   /*
    * Checked for stackoverflow condition
    * val num1 = Range(1,100000).toList
   val num2 = Range(1,100000).toList
   val num3 = Range(1,100000).toList
   val num4 = Range(1,100000).toList
   concat(concat(concat(num1,num2),num3),num4).foreach(println)*/
 }
}