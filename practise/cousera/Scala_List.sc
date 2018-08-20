package org.scala.learning.cousera

object Scala_List {
  val fruit: List[String] = List("Apple", "Orange", "Banana")
                                                  //> fruit  : List[String] = List(Apple, Orange, Banana)
  val num: List[Int] = List(1, 2, 3, 4, 5)        //> num  : List[Int] = List(1, 2, 3, 4, 5)
  val diag: List[List[Int]] = List(List(1, 2, 3, 4), List(2, 3, 4), List(0, 1, 0))
                                                  //> diag  : List[List[Int]] = List(List(1, 2, 3, 4), List(2, 3, 4), List(0, 1, 0
                                                  //| ))
  val f = "a" :: ("b" :: ("c" :: Nil))            //> f  : List[String] = List(a, b, c)
  val t = "a" :: "b" :: "c" :: "d" :: "e" :: Nil  //> t  : List[String] = List(a, b, c, d, e)
  val m = List()                                  //> m  : List[Nothing] = List()
  val n = Range(1, 10)                            //> n  : scala.collection.immutable.Range = Range(1, 2, 3, 4, 5, 6, 7, 8, 9)
  f :: "e" :: Nil                                 //> res0: List[java.io.Serializable] = List(List(a, b, c), e)
  var g = Range(1, 10).foreach { x => x :: m }    //> g  : Unit = ()

  def ListPatter(list: List[Int]): String = list match {
    case 1 :: 2 :: xs  => "List Start with 1, 2"+xs
    case List(x)       => "Same as X::Nil"
    case x :: Nil      => "List contain single element"
    case List()        => "Empty List"
  }                                               //> ListPatter: (list: List[Int])String
  
  ListPatter(List(1,2,3))                         //> res1: String = List Start with 1, 2List(3)
  ListPatter(List(1))                             //> res2: String = Same as X::Nil
  ListPatter(List())                              //> res3: String = Empty List
  
  val lt = List(1,4,3	,2,7,8)                   //> lt  : List[Int] = List(1, 4, 3, 2, 7, 8)
  lt.length                                       //> res4: Int = 6
  lt take 3                                       //> res5: List[Int] = List(1, 4, 3)
  lt take 0                                       //> res6: List[Int] = List()
  lt drop 3                                       //> res7: List[Int] = List(2, 7, 8)
  lt(5)                                           //> res8: Int = 8
  
  val (fend,send) = lt splitAt 3                  //> fend  : List[Int] = List(1, 4, 3)
                                                  //| send  : List[Int] = List(2, 7, 8)
  
  val xs = List(10,20,50,60,80,30)                //> xs  : List[Int] = List(10, 20, 50, 60, 80, 30)
  lt ++ xs reverse                                //> res9: List[Int] = List(30, 80, 60, 50, 20, 10, 8, 7, 2, 3, 4, 1)
  
  val xs1 = xs updated (1,100)                    //> xs1  : List[Int] = List(10, 100, 50, 60, 80, 30)
  xs==xs1                                         //> res10: Boolean = false
  xs contains 10                                  //> res11: Boolean = true
  xs contains 200                                 //> res12: Boolean = false
  
  xs indexOf 80                                   //> res13: Int = 4
  xs indexOf 123                                  //> res14: Int = -1
  xs init                                         //> res15: List[Int] = List(10, 20, 50, 60, 80)
  val w=List(10)                                  //> w  : List[Int] = List(10)
  w init                                          //> res16: List[Int] = List()
}