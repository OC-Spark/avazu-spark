package org.ocspark.avazu.ensemble.util

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.ocspark.avazu.Common

object GenData {

  def main(args: Array[String]): Unit = {
    val tr_src_path = args(0)
    val va_src_path = args(1)
    val tr_app_dst_path = args(2)
    val va_app_dst_path = args(3)
    val tr_site_dst_path = args(4)
    val va_site_dst_path = args(5)
  }

  val SIMPLE_FIELDS = List[String]("id", "click", "hour", "banner_pos", "device_id", "device_ip", "device_model", "device_type", "device_conn_type", "C1", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21")
  val NEW_FIELDS = SIMPLE_FIELDS ++ List[String]("pub_id", "pub_domain", "pub_category", "device_id_cnt", "device_ip_cnt", "user_cnt", "user_hour_cnt", "user_click_history", "sp1_cnt", "user_click_history2")

  val simpleFieldMap = Map[String, Int]("id" -> 0, "click" -> 1, "hour" -> 2, "banner_pos" -> 3, "device_id" -> 4, "device_ip" -> 5, "device_model" -> 6, "device_type" -> 7, "device_conn_type" -> 8, "C1" -> 9, "C14" -> 10, "C15" -> 11,
    "C16" -> 12, "C17" -> 13, "C18" -> 14, "C19" -> 15, "C20" -> 16, "C21" -> 17)

  val newFieldMap = Map[String, Int]("id" -> 0, "click" -> 1, "hour" -> 2, "banner_pos" -> 3, "device_id" -> 4, "device_ip" -> 5, "device_model" -> 6, "device_type" -> 7, "device_conn_type" -> 8, "C1" -> 9, "C14" -> 10, "C15" -> 11,
    "C16" -> 12, "C17" -> 13, "C18" -> 14, "C19" -> 15, "C20" -> 16, "C21" -> 17, "pub_id" -> 18, "pub_domain" -> 19, "pub_category" -> 20, "device_id_cnt" -> 21, "device_ip_cnt" -> 22, "user_cnt" -> 23, "user_hour_cnt" -> 24,
    "user_click_history" -> 25, "sp1_cnt" -> 26, "user_click_history2" -> 27)

  var id_cnt = Map[String, Int]()
  var ip_cnt = Map[String, Int]()
  var user_cnt = Map[String, Int]()
  var user_hour_cnt = Map[String, Int]()
  var sp1_cnt = Map[String, Int]()

  def run(tr_src_path: String, va_src_path: String, tr_dst_path: String, va_dst_path: String, sc: SparkContext) {
    println("=====GenData scanning=====")
    val lines = sc.textFile(tr_src_path, 4)
    val headers = lines.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.take(1) else iter.take(0) }
    .map {
      line =>
        line.split(",")
    }.flatMap ( h => h)
    .collect.toList
    
    val headerHashMap = collection.mutable.HashMap[String, Int]()
    var i = 0
    headers.foreach {
      header =>
        headerHashMap.put(header, i)
        i += 1
    }
    val headerMap = headerHashMap.toMap
    
    val srcData = lines.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map { line => line.split(",")}
    srcData.cache
    val user_cntRDD = userCount(srcData)
    val user_hour_cntRDD = userHourCount(srcData)
    val id_cntRDD = idCount(srcData, headerMap)
    val ip_cntRDD = ipCount(srcData, headerMap)
    val sp1_cntRDD = splCount(srcData, headerMap)
    
    val vsRows = sc.textFile(va_src_path, 4)
    val vaSrcData = lines.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map { line => line.split(",")}
    vaSrcData.cache
    val va_user_cntRDD = userCount(vaSrcData)
    val va_user_hour_cntRDD = userHourCount(vaSrcData)
    val va_id_cntRDD = idCount(vaSrcData, headerMap)
    val va_ip_cntRDD = ipCount(vaSrcData, headerMap)
    val va_sp1_cntRDD = splCount(vaSrcData, headerMap)

    user_cnt = user_cntRDD.union(va_user_cntRDD).reduceByKey(_ + _).collect.toMap
    user_hour_cnt = user_hour_cntRDD.union(va_user_hour_cntRDD).reduceByKey(_ + _).collect.toMap
    id_cnt = id_cntRDD.union(va_id_cntRDD).reduceByKey(_ + _).collect.toMap
    ip_cnt = ip_cntRDD.union(va_ip_cntRDD).reduceByKey(_ + _).collect.toMap
    sp1_cnt = sp1_cntRDD.union(va_sp1_cntRDD).reduceByKey(_ + _).collect.toMap

    val idCntBC = sc.broadcast(id_cnt)
    val ipCntBC = sc.broadcast(ip_cnt)
    val userCnt = sc.broadcast(user_cnt)
    val userHourCnt = sc.broadcast(user_hour_cnt)
    val splCnt = sc.broadcast(sp1_cnt)

    println("=====GenData generating=====")
    val trData = gen_data(srcData, true, idCntBC, ipCntBC, userCnt, userHourCnt, splCnt, headerMap, sc).collect
    Common.writeOut(NEW_FIELDS.toArray, trData, tr_dst_path)
    val vaData = gen_data(srcData, false, idCntBC, ipCntBC, userCnt, userHourCnt, splCnt, headerMap, sc).collect
    Common.writeOut(NEW_FIELDS.toArray, vaData, va_dst_path)
  }

  def def_spl(row: Array[String], headerMap : Map[String, Int]): String = {
    val sp1 = ArrayBuffer[String]()
    for (field <- List[String]("hour", "banner_pos", "device_id", "device_ip", "device_model", "device_type", "device_conn_type", "C1", "C14", "C15", "C16", "C17", "C18", "C19", "C20", "C21", "app_id", "app_domain", "app_category", "site_id", "site_domain", "site_category")) {
      sp1.append(row(headerMap(field)))
    }
    sp1.toString
  }

  def idCount(rows: RDD[Array[String]], headerMap : Map[String, Int]): RDD[(String, Int)] = {
    val idCount = rows.map {
      row =>
        (row(headerMap("device_id")), 1)
    }
    .reduceByKey(_ + _)

    idCount
  }

  def ipCount(lines: RDD[Array[String]], headerMap : Map[String, Int]): RDD[(String, Int)] = {
    val ipCount = lines.map {
      row =>
        (row(headerMap("device_ip")), 1)
    }
      .reduceByKey(_ + _)

    ipCount
  }

  def userCount(lines: RDD[Array[String]]): RDD[(String, Int)] = {
    val userCount = lines.map {
      row =>
        val user = Common.defUser(row)
        (user, 1)
    }
      .reduceByKey(_ + _)

    userCount
  }

  def userHourCount(lines: RDD[Array[String]]): RDD[(String, Int)] = {
    val userHourCount = lines.map {
      row =>
        val userHour = defUserHour(row)
        (userHour, 1)
    }
    .reduceByKey(_ + _)

    userHourCount
  }
  
  def defUserHour(row: Array[String]) : String = {
    Common.defUser(row) + "-" + row(GenData.simpleFieldMap("hour"))
  }

  def splCount(lines: RDD[Array[String]], headerMap : Map[String, Int]) : RDD[(String, Int)] = {
    val splCount = lines.map {
      row =>
        val spl = def_spl(row, headerMap)
        (spl, 1)
    }
      .reduceByKey(_ + _)

    splCount
  }

  val history = collection.mutable.HashMap[String, UserHistory]()
  val history2 = collection.mutable.HashMap[String, UserHistory]()

  def gen_data(srcData: RDD[Array[String]], is_train: Boolean, idCntBC: Broadcast[Map[String, Int]], ipCntBC: Broadcast[Map[String, Int]], userCntBC: Broadcast[Map[String, Int]],
    userHourCntBC: Broadcast[Map[String, Int]], splCntBC: Broadcast[Map[String, Int]], headerMap : Map[String, Int], sc: SparkContext) : RDD[String] = {
//    val in = sc.textFile(src_path, 4)
//    val out = sc.textFile(dst_path, 4)

    val id_cnt = idCntBC.value
    val ip_cnt = ipCntBC.value
    val user_cnt = userCntBC.value
    val user_hour_cnt = userHourCntBC.value
    val spl_cnt = splCntBC.value

    val newRowRDD = srcData.map {
      row =>
        val new_row = new Array[String](NEW_FIELDS.length)
        for (field <- SIMPLE_FIELDS) {
           setNewRow(field, row(headerMap(field)), new_row)
        }

        setNewRow("device_id_cnt", id_cnt(row(headerMap("device_id"))).toString, new_row)
        setNewRow("device_ip_cnt", ip_cnt(row(headerMap("device_ip"))).toString, new_row)

        val user = Common.defUser(row)
        val hour = row(headerMap("hour"))
        val userHour = defUserHour(row)
        val spl = def_spl(row, headerMap)
        
        setNewRow("user_cnt", user_cnt(user).toString, new_row)
        setNewRow("user_hour_cnt", user_hour_cnt(userHour).toString, new_row)
        setNewRow("sp1_cnt", sp1_cnt(spl).toString, new_row)

        if (has_id_info(row)) {
          val userHistory = getUserHistory(history, user)
          if (userHistory.prevHour != hour) {
            userHistory.history != userHistory.history + userHistory.getLast4Buffer
            userHistory.buffer = ""
            userHistory.prevHour = hour
          }
          setNewRow("user_click_history", userHistory.history, new_row)

          if (is_train) {
            userHistory.buffer += row(headerMap("click"))
          }
        } else {
          val userHistory2 = getUserHistory(history2, user)
          if (userHistory2.prevHour != hour) {
            userHistory2.history != userHistory2.history + userHistory2.getLast4Buffer
            userHistory2.buffer = ""
            userHistory2.prevHour = hour
          }
          setNewRow("user_click_history2", userHistory2.history, new_row)

          if (is_train) {
            userHistory2.buffer += row(headerMap("click"))
          }
        }

        if (Common.is_app(row(headerMap("site_id")))) {
          setNewRow("pub_id", row(headerMap("app_id")), new_row)
          println("appid => " + row(headerMap("app_id")))
          setNewRow("pub_domain", row(headerMap("app_domain")), new_row)
          setNewRow("pub_category", row(headerMap("app_category")), new_row)
        } else {
          println("siteid => " + row(headerMap("site_id")))
          setNewRow("pub_id", row(headerMap("site_id")), new_row)
          setNewRow("pub_domain", row(headerMap("site_domain")), new_row)
          setNewRow("pub_category", row(headerMap("site_category")), new_row)
        }

        new_row.mkString(",")
    }
    newRowRDD
  }
  
  def has_id_info(row: Array[String]): Boolean = {
    val returnValue = if (row(simpleFieldMap("device_id")) == "a99f214a") false else true
    returnValue
  }

  def setNewRow(field: String, value: String, newRow: Array[String]) {
    newRow(newFieldMap(field)) = value
  }

  def getUserHistory(history: collection.mutable.HashMap[String, UserHistory], user: String): UserHistory = {
    history.get(user) match {
      case Some(userHistory) => userHistory
      case None =>
        val userHistory = new UserHistory()
        history.put(user, userHistory)
        userHistory
    }
  }

  class UserHistory() {
    var prevHour = ""
    var history = ""
    var buffer = ""

    def getLast4Buffer() {
      var last4 = ""
      val bufferLen = buffer.length()
      if (bufferLen >= 4) {
        last4 = buffer.substring(bufferLen - 4)
      } else {
        last4 = buffer
      }
      last4
    }

  }

}