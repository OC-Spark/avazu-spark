package org.ocspark.avazu.bag.util

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.ocspark.avazu.Common

object JoinData {

  def main(args: Array[String]): Unit = {
    val size = "x"
    val category = "app"
      
    val sparkConf = new SparkConf().setAppName("Convert6")
      .setMaster("local[1]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)
    
    JoinData.run(s"/avazu/bag/tr.r$size.$category.sp", s"/avazu/bag/tr.r$size.$category.new.csv.group", sc)
  }
  
  def run(raw : String, join : String, sc : SparkContext){
//	println("raw = " + raw)
	val to_join = collection.mutable.HashMap[String, String]()
	
	val x = sc.textFile("hdfs://" + Common.hdfsHost + join).map {
	  line =>
	    val x = line.trim().split(" ", 2)
	    x
	}
//	x.count		// trigger command
	x.collect.foreach{
	  x => 
	  	to_join.put(x(0), if (x.length == 2) x(1) else "")
//	    println("x0 = " + x(0) +", x1 =" + (if (x.length == 2) x(1) else ""))
	}
	println("to_join size = " + to_join.size)
//	for (key <- to_join.keySet){
//	  println("key:" + key +" :" + to_join.getOrElse(key, "none"))
//	}
	
	val joined = sc.textFile("hdfs://" + Common.hdfsHost + raw).map {
	  line =>
//	    println("join data line = " + line)
	    val rowId_raw = line.trim().split(" ", 2)
//	    println("rowId_raw length = " + rowId_raw.length)
//	    println("i = " + rowId_raw(0) + ", " + rowId_raw(1) )
//	    println(rowId_raw(1) + " " + to_join.getOrElse(rowId_raw(0), ""))
//	    println("to_join.getorelse(" + rowId_raw(0) + "=" + to_join.getOrElse(rowId_raw(0), "nothing"))
	    rowId_raw(1) + " " + to_join.getOrElse(rowId_raw(0), "")
	}
	
	val joinedString = joined.collect
//	joinedString.foreach(println)
	
	Common.writeOut(joinedString.mkString("\n"), raw + ".join")		// overwrite raw file

  }

}