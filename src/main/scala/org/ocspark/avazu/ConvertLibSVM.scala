package org.ocspark.avazu

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object ConvertLibSVM {

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    
    val sparkConf = new SparkConf().setAppName("ConvertLibSVM")
    .setMaster("local[4]")    // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)
    
    addOne(input, output, sc)
  }
  
  def addOne(input : String, output : String, sc : SparkContext){
    
    val inputRDD = sc.textFile("hdfs://" + Common.hdfsHost + "/" + input, 4)
    
    val outputRDD = inputRDD.map {
      line =>
      val words = line.split(" ")
      val length = words.length
      val label = words(1)
      
      val wordArray = new ArrayBuffer[Long]()
      
      for (i <- 2 until (length - 1)){
        wordArray.append(words(i).toLong)
      }
      val sb = new StringBuilder()
      sb.append(label)
      wordArray.sorted.foreach{
        long =>
          sb.append(" " + long + ":1")
      }
      sb.toString
    }
    
    Common.writeOut(Array[String](), outputRDD.collect, output)
    
  }
  
  def stripField(input : String, output : String, sc : SparkContext){
    val inputRDD = sc.textFile("hdfs://" + Common.hdfsHost + "/" + input, 4)
    
    val outputRDD = inputRDD.map {
      line =>
      val words = line.split(" ")
      val length = words.length
      val label = words(0)
      
      val wordArray = new ArrayBuffer[String]()
      wordArray.append(label)
      
      for (i <- 1 until (length - 1)){
//        println("words i = " + words(i))
//        if (words(i).contains(":")){
	        val parts = words(i).split(":")
	        wordArray.append(parts(1) + ":" + parts(2))		// skip first part which is field
//	    } else {
//	      println("word " + i + " is blank")
//	    }
      }
      wordArray.sortBy(combo => combo.split(":")(0).toInt).mkString(" ")
    }
    
    Common.writeOut(Array[String](), outputRDD.collect, output)
    
  }

}