package org.scala.learning.collection

object ScalaList {
  val fruit = List("appple", "pineapple", "banana")
                                                  //> fruit  : List[String] = List(appple, pineapple, banana)
  val fruit_param: List[String] = List("appple", "pineapple", "banana")
                                                  //> fruit_param  : List[String] = List(appple, pineapple, banana)
  val daig: List[List[Int]] = List(List(1, 2, 3), List(34, 38, 45), List(30, 56, 78))
                                                  //> daig  : List[List[Int]] = List(List(1, 2, 3), List(34, 38, 45), List(30, 56,
                                                  //|  78))
  val f = "a" :: ("b" :: ("c" :: Nil))            //> f  : List[String] = List(a, b, c)
  val t = "a" :: "b" :: "c" :: "d" :: Nil         //> t  : List[String] = List(a, b, c, d)
  f :: t                                          //> res0: List[java.io.Serializable] = List(List(a, b, c), a, b, c, d)
  f :: t :: Nil                                   //> res1: List[List[String]] = List(List(a, b, c), List(a, b, c, d))


  def ListPatterMatch(list: List[Int]) = list match {
    case 1 :: 2 :: xs => "List start with 1 and 2 remaining xs: " + xs
    case List(x)      => "List with single element same as x::Nil"
    case x :: Nil     => "List with single element"
    case List()       => "Empty list"
  }                                               //> ListPatterMatch: (list: List[Int])String

  ListPatterMatch(List())                         //> res2: String = Empty list
  ListPatterMatch(List(1))                        //> res3: String = List with single element same as x::Nil
  ListPatterMatch(List(1, 2))                     //> res4: String = List start with 1 and 2 remaining xs: List()
  ListPatterMatch(List(1, 2, 3, 4, 5, 6))         //> res5: String = List start with 1 and 2 remaining xs: List(3, 4, 5, 6)

  val lt = List(10, 20, 30, 40, 50, 60, 70, 80, 90)
                                                  //> lt  : List[Int] = List(10, 20, 30, 40, 50, 60, 70, 80, 90)
  lt.length                                       //> res6: Int = 9
  lt take 0                                       //> res7: List[Int] = List()
  lt take -1                                      //> res8: List[Int] = List()
  lt take 10                                      //> res9: List[Int] = List(10, 20, 30, 40, 50, 60, 70, 80, 90)
  lt take 5                                       //> res10: List[Int] = List(10, 20, 30, 40, 50)

  lt drop 0                                       //> res11: List[Int] = List(10, 20, 30, 40, 50, 60, 70, 80, 90)
  lt drop -1                                      //> res12: List[Int] = List(10, 20, 30, 40, 50, 60, 70, 80, 90)
  lt drop 10                                      //> res13: List[Int] = List()
  lt drop 5                                       //> res14: List[Int] = List(60, 70, 80, 90)
  lt reverse                                      //> res15: List[Int] = List(90, 80, 70, 60, 50, 40, 30, 20, 10)

  lt updated (2, 100)                             //> res16: List[Int] = List(10, 20, 100, 40, 50, 60, 70, 80, 90)
  lt updated (0, 100)                             //> res17: List[Int] = List(100, 20, 30, 40, 50, 60, 70, 80, 90)
  //lt updated (-1,100)
  //lt updated (10, 100)
  val lt1 = lt updated (1, 123)                   //> lt1  : List[Int] = List(10, 123, 30, 40, 50, 60, 70, 80, 90)
  lt == lt1                                       //> res18: Boolean = false
  lt contains 10                                  //> res19: Boolean = true
  lt contains 100                                 //> res20: Boolean = false

  lt indexOf 10                                   //> res21: Int = 0
  lt indexOf 123                                  //> res22: Int = -1
  lt init                                         //> res23: List[Int] = List(10, 20, 30, 40, 50, 60, 70, 80)

  val ys = List(10)                               //> ys  : List[Int] = List(10)
  ys init                                         //> res24: List[Int] = List()

  val zx = List()                                 //> zx  : List[Nothing] = List()
  //zx init
  val xs = List(11, 22, 33, 44, 55, 66, 77, 88, 99)
                                                  //> xs  : List[Int] = List(11, 22, 33, 44, 55, 66, 77, 88, 99)
  xs reverse                                      //> res25: List[Int] = List(99, 88, 77, 66, 55, 44, 33, 22, 11)

  lt ++ xs                                        //> res26: List[Int] = List(10, 20, 30, 40, 50, 60, 70, 80, 90, 11, 22, 33, 44,
                                                  //|  55, 66, 77, 88, 99)

  def scaleList(f: Int => Int, xs: List[Int]): List[Int] = xs match {
    case Nil     => xs
    case y :: ys => f(y) :: scaleList(f, ys)
  }                                               //> scaleList: (f: Int => Int, xs: List[Int])List[Int]

  val sclt = scaleList(x => x * 2, lt)            //> sclt  : List[Int] = List(20, 40, 60, 80, 100, 120, 140, 160, 180)
  val sclt1 = scaleList(x => x * x, lt)           //> sclt1  : List[Int] = List(100, 400, 900, 1600, 2500, 3600, 4900, 6400, 8100
                                                  //| )
  def posElem(f: Int => Boolean, xs: List[Int]): List[Int] = xs match {
    case Nil     => xs
    case y :: ys => if (f(y)) y :: posElem(f, ys) else posElem(f, ys)
  }                                               //> posElem: (f: Int => Boolean, xs: List[Int])List[Int]
  val poslt = posElem(x => x % 3 == 0, lt)        //> poslt  : List[Int] = List(30, 60, 90)
  val poslt1 = posElem(x => x % 2 != 0, lt)       //> poslt1  : List[Int] = List()

  val nums = List(2, 4, -1, -4, 7, 8, 9, 10)      //> nums  : List[Int] = List(2, 4, -1, -4, 7, 8, 9, 10)

  nums filter (x => x > 0)                        //> res27: List[Int] = List(2, 4, 7, 8, 9, 10)
  nums filterNot (x => x > 0)                     //> res28: List[Int] = List(-1, -4)
  val tup = nums partition (x => x > 0)           //> tup  : (List[Int], List[Int]) = (List(2, 4, 7, 8, 9, 10),List(-1, -4))
  tup._1                                          //> res29: List[Int] = List(2, 4, 7, 8, 9, 10)
  tup._2                                          //> res30: List[Int] = List(-1, -4)
  nums takeWhile (x => x > 0)                     //> res31: List[Int] = List(2, 4)
  nums dropWhile (x => x > 0)                     //> res32: List[Int] = List(-1, -4, 7, 8, 9, 10)
  val tup1 = nums span (x => x > 0)               //> tup1  : (List[Int], List[Int]) = (List(2, 4),List(-1, -4, 7, 8, 9, 10))
  tup1._1                                         //> res33: List[Int] = List(2, 4)
  tup1._2                                         //> res34: List[Int] = List(-1, -4, 7, 8, 9, 10)

  def pack[T](xs: List[T]): List[List[T]] = xs match {
    case Nil => Nil
    case x :: xs1 =>
      val (first, rest) = xs span (y => y == x)
      first :: pack(rest)
  }                                               //> pack: [T](xs: List[T])List[List[T]]

  val data = List("a", "a", "a", "a", "b", "b", "b", "c", "c", "c", "a", "a")
                                                  //> data  : List[String] = List(a, a, a, a, b, b, b, c, c, c, a, a)
  pack(data)                                      //> res35: List[List[String]] = List(List(a, a, a, a), List(b, b, b), List(c, c
                                                  //| , c), List(a, a))

  def encode[T](xs: List[T]): List[(T, Int)] = xs match {
    case Nil => Nil
    case x :: xs1 =>
      val (first, rest) = xs span (y => y == x)
      (first(0), first.length) :: encode(rest)
  }                                               //> encode: [T](xs: List[T])List[(T, Int)]
  
  def encodeUsingPackFuction[T](xs:List[T]):List[(T,Int)] = {
      pack(xs) map (ys => (ys.head,ys.length))
  }                                               //> encodeUsingPackFuction: [T](xs: List[T])List[(T, Int)]
  encode(data)                                    //> res36: List[(String, Int)] = List((a,4), (b,3), (c,3), (a,2))
  encodeUsingPackFuction(data)                    //> res37: List[(String, Int)] = List((a,4), (b,3), (c,3), (a,2))
}