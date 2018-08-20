package org.scala.learning.cousera

class Employee(val id: Int, val name: String, val salary: Double) {

  def TDS = salary * 0.02
  
  override def toString()="Id: "+this.id+" name: "+this.name+" salary: "+this.salary+" TDS: "+TDS
}
object Employee{
  def apply(id:Int,name: String,salary:Double)={
    new Employee(id,name,salary)
  }
}