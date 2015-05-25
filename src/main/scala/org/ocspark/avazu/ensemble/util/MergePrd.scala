package org.ocspark.avazu.ensemble.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import org.ocspark.avazu.Common

object MergePrd {

  def main(args: Array[String]): Unit = {
    val prd_path = args(0)
    val out_path = args(1)
  }
  
  def run(out_path : String, sc : SparkContext){
    val prd_path = "/avazu/pool/*"
    val prds = sc.textFile(prd_path, 4)
    .filter ( line => !line.contains("id"))
    .map {
      line => 
        val row = line.split(",")
//        println("prd = " + row(1))
        (row(0), row(1).toDouble)
    }
    .groupByKey()
    .map {
      id_list =>
        val id = id_list._1
        val list = id_list._2
        val averagePrd = Common.averagePrd(list)
//        println("av prd = " + averagePrd)
        (id, averagePrd)
    }
    write_prd(prds, out_path)
  }

   def write_prd(prds : RDD[(String, Double)], dst_path : String){
     val lines = ArrayBuffer[String]()
     lines.append("id,click")
     prds.collect.foreach {
       id_prd =>
         lines.append(id_prd._1 + "," + id_prd._2)
     }
     Common.writeOut(lines.mkString("\n"), dst_path)
   }
}