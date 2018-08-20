package org.scala.learning.datastructure

object test {
  def nth[T](n: Int, xs: List[T]): T =
    if (xs.isEmpty) throw new IndexOutOfBoundsException
    else if (n == 0) xs.head
    else nth(n - 1, xs.tail)                      //> nth: [T](n: Int, xs: org.scala.learning.datastructure.List[T])T

  val xs = new Cons(1, new Cons(2, new Cons(3, new Nil)))
                                                  //> xs  : org.scala.learning.datastructure.Cons[Int] = org.scala.learning.datast
                                                  //| ructure.Cons@880ec60
  nth(2, xs)                                      //> res0: Int = 3
 // nth(4,xs)
  nth(-1,xs)                                      //> java.lang.IndexOutOfBoundsException
                                                  //| 	at org.scala.learning.datastructure.test$$anonfun$main$1.nth$1(org.scala
                                                  //| .learning.datastructure.test.scala:5)
                                                  //| 	at org.scala.learning.datastructure.test$$anonfun$main$1.apply$mcV$sp(or
                                                  //| g.scala.learning.datastructure.test.scala:12)
                                                  //| 	at org.scalaide.worksheet.runtime.library.WorksheetSupport$$anonfun$$exe
                                                  //| cute$1.apply$mcV$sp(WorksheetSupport.scala:76)
                                                  //| 	at org.scalaide.worksheet.runtime.library.WorksheetSupport$.redirected(W
                                                  //| orksheetSupport.scala:65)
                                                  //| 	at org.scalaide.worksheet.runtime.library.WorksheetSupport$.$execute(Wor
                                                  //| ksheetSupport.scala:75)
                                                  //| 	at org.scala.learning.datastructure.test$.main(org.scala.learning.datast
                                                  //| ructure.test.scala:3)
                                                  //| 	at org.scala.learning.datastructure.test.main(org.scala.learning.datastr
                                                  //| ucture.test.scala)
}