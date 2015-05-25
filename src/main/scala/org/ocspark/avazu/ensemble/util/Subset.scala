package org.ocspark.avazu.ensemble.util

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.ListBuffer
import scala.annotation.meta.field
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.ocspark.avazu.Common
import scala.util.control._

object Subset {
    val filterHashMap = collection.mutable.HashMap[String, ListBuffer[String]]()
    val inv_filterHashMap = collection.mutable.HashMap[String, ListBuffer[String]]()
    val cnt_filterHashMap = collection.mutable.HashMap[String, ListBuffer[Int]]()
    val inv_end_filterHashMap = collection.mutable.HashMap[String, ListBuffer[String]]()
//    val cnt = collection.mutable.HashMap[String, Int]()

  def main(args: Array[String]): Unit = {
    	
  }

  def run(filterString: String, tr_src_path: String, va_src_path: String, tr_dst_path: String, va_dst_path: String, sc: SparkContext) {

    var cold_user_filter = false

    for (token <- filterString.split(",")) {
      if (token.startsWith("D")) {
        cold_user_filter = true
      } else {
        val tokenParts = token.split("-")
        val tType = tokenParts(0)
        val field = tokenParts(1)
        val value = tokenParts(2)
        if (tType == "A") {
          fillFilter(filterHashMap, field, value)
        } else if (tType == "B") {
          fillFilter(inv_filterHashMap, field, value)
        } else if (tType == "C") {
          fillIntFilter(cnt_filterHashMap, field, value.toInt)
        } else if (tType == "E") {
          fillFilter(inv_end_filterHashMap, field, value)
        } else {
          print("unknown filter type")
          exit(1)
        }
      }
    }
    
    val filter = filterHashMap.toMap
    val inv_filter = inv_filterHashMap.toMap
    val cnt_filter = cnt_filterHashMap.toMap
    val inv_end_filter = inv_end_filterHashMap.toMap
    
    var cnt : Map[String, Int] = Map[String, Int]()
    
    if (cnt_filter.size != 0){
 	    println("=====scanning=====")
	    val cntRDD = scan(tr_src_path, cnt_filter, sc)
	    val cntRDD1 = scan(va_src_path, cnt_filter, sc)
	    cnt = cntRDD.union(cntRDD1).reduceByKey(_ + _).collect.toMap
    }

    var user_set : Set[String] = Set[String]()
    
	if (cold_user_filter){
	    println("=====scanning user=====")
	    user_set = scan_user(tr_src_path, sc).collect.toSet
	}

	println("=====subsetting=====")
	val srcRDD = sc.textFile(tr_src_path, 4)
    srcRDD.cache
    
    val headerMutableMap = collection.mutable.HashMap[String, Int]()
    val headerList = ListBuffer[String]()
    srcRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.take(1) else iter.take(0) }
    .collect
    .foreach{
      line => 
        val row = line.split(",")
	    var i = 0
        for (word <- row){
	        headerMutableMap.put(word, i)
	        headerList.append(word)
	        i += 1
        }
      }
    
    val headerMap = headerMutableMap.toMap
    
    val srcData = srcRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
	val trFiltered = subset(srcData, headerMap, true, filter, inv_filter, cnt_filter, inv_end_filter, cold_user_filter, cnt, user_set, sc)
	
	val headerArray = headerList.toArray
	Common.writeOut(headerArray, trFiltered.collect, tr_dst_path)
	
	val vaSrcRDD = sc.textFile(va_src_path, 4)
	val vaSrcData = vaSrcRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
	val vaFiltered = subset(vaSrcData, headerMap, false, filter, inv_filter, cnt_filter, inv_end_filter, cold_user_filter, cnt, user_set, sc)
	Common.writeOut(headerArray, vaFiltered.collect, va_dst_path)

  }

  def fillFilter(map: collection.mutable.HashMap[String, ListBuffer[String]], field: String, value: String) {
    val valueList = map.getOrElse(field, ListBuffer[String]())
    if (valueList.size == 0) {
      map.put(field, valueList)
    }
    valueList += value
  }
  
    def fillIntFilter(map: collection.mutable.HashMap[String, ListBuffer[Int]], field: String, value: Int) {
    val valueList = map.getOrElse(field, ListBuffer[Int]())
    if (valueList.size == 0) {
      map.put(field, valueList)
    }
    valueList += value
  }
  
  def scan(path : String, cnt_filter : Map[String, ListBuffer[Int]] , sc : SparkContext): RDD[(String, Int)] = {
    val rowRDD =sc.textFile(path, 4)
    rowRDD.cache
    
    val headerMap = collection.mutable.HashMap[String, Int]()
    val header = rowRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.take(1) else null }
    .collect
    .foreach{
      c => 
        var i = 0
        headerMap.put(c, i)
        i += 1
      }
    
//    val headerMapBroadCast = sc.broadcast(headerMap)
    
    val fieldCnt = rowRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    .map {
      line =>
        val words = line.split(",")		// , or space?
        val cntKeyArray = ArrayBuffer[String]()
        for (field <- cnt_filter.keySet){
          cntKeyArray.append(field + "-" + words(headerMap.getOrElse(field, 0)))
        }
        cntKeyArray
    }
    .flatMap {
      t => t
    }
    .map {
      key =>
        (key, 1)
    }
    .reduceByKey(_ + _)
    
    fieldCnt
  }
  
  def scan_user(path: String, sc : SparkContext) : RDD[String] = {
    val userRDD = sc.textFile(path, 4)
    .map {
      line =>
        val row = line.split(",")
        val user = Common.defUser(row)
        
        user
    }.distinct
    
    userRDD
  }
  
  def filterA(filter: Map[String, ListBuffer[String]], headerMap : Map[String, Int], row : Array[String]) : Boolean = {
    var filterMatch = true
    for (filterKey <- filter.keySet){
          val values = filter.getOrElse(filterKey, ListBuffer[String]())
          val valueLoop = new Breaks
            valueLoop.breakable {
              for (value <- values) {
                val index = headerMap(filterKey)
                if (!row(index).startsWith(value)) {
                  filterMatch = false
                  valueLoop.break
                }
              }
            }
        }
    
    filterMatch
  }
  
  def filterB(inv_end_filter: Map[String, ListBuffer[String]], headerMap : Map[String, Int], row : Array[String]) : Boolean = {
    var filterMatch = true
    for (filterKey <- inv_end_filter.keySet) {
              val values = inv_end_filter.getOrElse(filterKey, ListBuffer[String]())
              val valueLoop = new Breaks
              valueLoop.breakable {
                for (value <- values) {
                  val index = headerMap(filterKey)
                  if (row(index).startsWith(value)) {
                    filterMatch = false
                    valueLoop.break
                  }
                }
              }
            }
    
    filterMatch
  } 
  
  def filterC(cnt_filter: Map[String, ListBuffer[Int]], cnt : Map[String, Int], headerMap : Map[String, Int], row : Array[String]) : Boolean = {
    var filterMatch = true
    for (filterKey <- cnt_filter.keySet) {
              val values = cnt_filter.getOrElse(filterKey, ListBuffer[Int]())
              val valueLoop = new Breaks
              valueLoop.breakable {
                for (value <- values) {
                  val index = headerMap(filterKey)
                   if (cnt(filterKey + row(index)) != value) {
                    filterMatch = false
                    valueLoop.break
                  }
                }
              }
            }
    filterMatch
  } 
  
  def filterE(inv_filter: Map[String, ListBuffer[String]], headerMap : Map[String, Int], row : Array[String]) : Boolean = {
    var filterMatch = true
    for (filterKey <- inv_filter.keySet) {
              val values = inv_filter.getOrElse(filterKey, ListBuffer[String]())
              val valueLoop = new Breaks
              valueLoop.breakable {
                for (value <- values) {
                  val index = headerMap(filterKey)
                  if (row(index).startsWith(value)) {
                    filterMatch = false
                    valueLoop.break
                  }
                }
              }
            }
    filterMatch
  } 
  
  def filterColdUser(cold_user_filter: Boolean, is_train : Boolean, user_set : Set[String] ,headerMap : Map[String, Int], row : Array[String]) : Boolean = {
    var filterMatch = true
    if (cold_user_filter & ! is_train){
             val user = Common.defUser(row)
            if (!user_set.contains(user)){
                filterMatch = false
            }
          }
    filterMatch
  }  
  
  def subset(srcData : RDD[String], headerMap : Map[String, Int], is_train : Boolean, filter : Map[String, ListBuffer[String]], inv_filter: Map[String, ListBuffer[String]], cnt_filter: Map[String, ListBuffer[Int]], 
      inv_end_filter: Map[String, ListBuffer[String]], cold_user_filter : Boolean, cnt : Map[String, Int], user_set : Set[String], sc : SparkContext) : RDD[String] = {

    val filteredData = srcData.map {
      line =>
        val row = line.split(",")
        row
    }
    .filter (row => filterA(filter, headerMap, row) & filterB(inv_filter, headerMap, row) & filterC(cnt_filter, cnt, headerMap, row)  & filterE(inv_end_filter, headerMap, row) & filterColdUser(cold_user_filter, is_train, user_set, headerMap, row))
      
    val line = filteredData.map {
      row =>
        row.mkString(",")		// back to line
    }
    line
  }
  
}