package org.scala.learning.cousera

trait Expr
case class Number(n: Int) extends Expr
case class Sum(e1: Expr, e2: Expr) extends Expr

object Expression {
  def show(e: Expr): String = e match {
    case Number(x) => x.toString
    case Sum(l, r) => show(l) + " + " + show(r)
  }
  def main(args: Array[String]): Unit = {
    println(show(Sum(Number(1), Number(44))))
  }
}
