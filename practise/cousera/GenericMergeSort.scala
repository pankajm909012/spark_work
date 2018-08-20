package org.scala.learning.cousera

object GenericMergeSort {
  def mergeSort[T](xs: List[T])(lt: (T, T) => Boolean): List[T] = {
    val n = xs.length / 2
    if (n == 0) xs
    else {
      def merge(xs: List[T], ys: List[T]): List[T] = (xs, ys) match {
        case (Nil, ys)            => ys
        case (xs, Nil)            => xs
        case (x :: xs1, y :: ys1) => if (lt(x, y)) x :: merge(xs1, ys) else y :: merge(xs, ys1)
      }
      val (fend,send) = xs splitAt n
      merge(mergeSort(fend)(lt),mergeSort(send)(lt))
    }
  }
  def main(args:Array[String]):Unit={
    val nums = List(10,-1,0,2,4,5,9,8,7)
    println(mergeSort(nums)((x: Int, y: Int) => x < y))
    
    val fruit =List("apple","orange","pineapple","banana")
    println(mergeSort(fruit)((x:String,y:String)=>x.compareTo(y)<0))
  }
}