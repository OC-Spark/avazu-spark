package org.ocspark.avazu.util

class Counter[T](inputArray: Array[T]) {
  val counts = new collection.mutable.HashMap[T, Int]();

  inputArray.foreach(s => add(s))
  
  def add(t: T) {
    val c = counts.getOrElse(t, 0)
    put(t, c + 1)
  }

  def count(t: T): Int = {
    counts.getOrElse(t, 0);
  }
  
  def items(): Iterator[(T, Int)] = {
    counts.iterator
  }
  
  def put(t : T, v : Int){
    counts.put(t, v)
  }

}