package org.scala.learning.datastructure

trait List[T] {
  def isEmpty: Boolean
  def head: T
  def tail: List[T]
}
