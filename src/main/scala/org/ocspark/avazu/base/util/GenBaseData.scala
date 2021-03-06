package org.ocspark.avazu.base.util

import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import javax.rmi.CORBA.GetORBPropertiesFileAction
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.ocspark.avazu.Common

object GenBaseData {
  val id = 0
  val click = 1
  val hour = 2
  val banner_pos = 3
  val device_id = 4
  val device_ip = 5
  val device_model = 6
  val device_conn_type = 7
  val C14 = 8
  val C17 = 9
  val C20 = 10
  val C21 = 11
  val pub_id = 12
  val pub_domain = 13
  val pub_category = 14
  val device_id_count = 15
  val device_ip_count = 16
  val user_count = 17
  val smooth_user_hour_count = 18
  val user_click_history = 19
  val newFieldMap = Map("id" -> 0, "click" -> 1, "hour" -> 2, "banner_pos" -> 3, "device_id" -> 4, "device_ip" -> 5, "device_model" -> 6, "device_conn_type" -> 7, "C14" -> 8, "C17" -> 9,
    "C20" -> 10, "C21" -> 11, "pub_id" -> 12, "pub_domain" -> 13, "pub_category" -> 14, "device_id_count" -> 15, "device_ip_count" -> 16, "user_count" -> 17,
    "smooth_user_hour_count" -> 18, "user_click_history" -> 19)

  //  val hdfsHost = "devserver2.office.onescreeninc.com:8020"
  val FIELDS = Array("id", "click", "hour", "banner_pos", "device_id", "device_ip", "device_model", "device_conn_type", "C14", "C17", "C20", "C21") // qw fields
  val NEW_FIELDS = FIELDS ++ Array("pub_id", "pub_domain", "pub_category", "device_id_count", "device_ip_count", "user_count", "smooth_user_hour_count", "user_click_history")

  val start = new Date()

  val history = collection.mutable.HashMap[String, UserHistory]()

  def main(args: Array[String]): Unit = {
    val trSrcPath = args(0)
    val vaSrcPath = args(1)
    val trAppDstPath = args(2)
    val vaAppDstPath = args(3)
    val trSiteDstPath = args(4)
    val vaSiteDstPath = args(5)

    val sparkConf = new SparkConf().setAppName("GenData")
      .setMaster("local[4]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)

    run(trSrcPath, vaSrcPath, trAppDstPath, vaAppDstPath, trSiteDstPath, vaSiteDstPath, sc)

  }

  def run(trSrcPath: String, vaSrcPath: String, trAppDstPath: String, vaAppDstPath: String, trSiteDstPath: String, vaSiteDstPath: String, sc: SparkContext) {

    val trSrcLines = sc.textFile("hdfs://" + Common.hdfsHost + trSrcPath, 4)
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map {
        line =>
          val row = line.split(",")
          row
      }
      .cache

    val tr_device_id_count = keyCount(trSrcLines, Common.device_id)
    val tr_device_ip_count = keyCount(trSrcLines, Common.device_ip)
    val tr_user_count = userCount(trSrcLines)
    val tr_user_hour_count = userHourCount(trSrcLines)

    val vaSrcLines = sc.textFile("hdfs://" + Common.hdfsHost + vaSrcPath, 4)
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map {
        line =>
          val row = line.split(",")
          row
      }
      .cache

    val va_device_id_count = keyCount(vaSrcLines, Common.device_id)
    val device_id_count = tr_device_id_count.union(va_device_id_count).reduceByKey(_ + _).collect.toMap

    val va_device_ip_count = keyCount(vaSrcLines, Common.device_ip)
    val device_ip_count = tr_device_ip_count.union(va_device_ip_count).reduceByKey(_ + _).collect.toMap

    val va_user_count = userCount(trSrcLines)
    val user_count = tr_user_count.union(va_user_count).reduceByKey(_ + _).collect.toMap

    val va_user_hour_count = userHourCount(trSrcLines)
    val user_hour_count = tr_user_hour_count.union(va_user_hour_count).reduceByKey(_ + _).collect.toMap

    val idMap = sc.broadcast(device_id_count)
    val ipMap = sc.broadcast(device_ip_count)
    val userMap = sc.broadcast(user_count)
    val userHourMap = sc.broadcast(user_hour_count)

    println("======================scan complete======================")

    gen_data(trSrcLines, trAppDstPath, trSiteDstPath, true, idMap, ipMap, userMap, userHourMap)
    gen_data(vaSrcLines, vaAppDstPath, vaSiteDstPath, false, idMap, ipMap, userMap, userHourMap)
  }

  def isHeader(line: String): Boolean = {
    line.contains("device_id")
  }

  def keyCount(rows: RDD[Array[String]], col: Int): RDD[(String, Int)] = {
    val idTuple = rows
      .map {
        row =>
          val device_id = row(col)
          (device_id, 1)
      }.reduceByKey(_ + _)
    idTuple
  }

  def userCount(rows: RDD[Array[String]]): RDD[(String, Int)] = {
    val userCount = rows.map {
      row =>
        val user = Common.defUser(row)
        (user, 1)
    }.reduceByKey(_ + _)
    userCount
  }

  def userHourCount(rows: RDD[Array[String]]): RDD[(String, Int)] = {
    val userHourCount = rows.map {
      row =>
        val (user, hour) = (Common.defUser(row), row(Common.hour))
        val userHour = user + "-" + hour
        (userHour, 1)
    }.reduceByKey(_ + _)
    userHourCount
  }
  
  def gen_data(rows: RDD[Array[String]], dst_app_path: String, dst_site_path: String, is_train: Boolean,
    idMap: Broadcast[Map[String, Int]], ipMap: Broadcast[Map[String, Int]],
    userCountMap: Broadcast[Map[String, Int]], userHourCountMap: Broadcast[Map[String, Int]]) {

    val appRows = rows.filter(row => Common.is_app(row(Common.site_id)))
      .map {
        row =>
          val newRow = new Array[String](NEW_FIELDS.size)

          for (field <- FIELDS) {
            val index = Common.fieldMap.get(field).get
            val newIndex = newFieldMap.get(field).get
            //            println("index = " + index +", newIndex" + newIndex)
            newRow(newIndex) = row(index)
          }

          newRow(device_id_count) = idMap.value.get(row(Common.device_id)).getOrElse(0) + ""
          newRow(device_ip_count) = ipMap.value.get(row(Common.device_ip)).getOrElse(0) + ""
          val (user, hour) = (Common.defUser(row), row(Common.hour))
          newRow(user_count) = userCountMap.value.get(user).getOrElse(0) + ""
          newRow(smooth_user_hour_count) = userHourCountMap.value.get(user + "-" + hour).getOrElse(0) + ""

          val userHistory = getUserHistory(history, user)
          if (has_id_info(row)) {
            if (userHistory.prevHour != hour) {
              userHistory.history != userHistory.history + userHistory.getLast4Buffer
              userHistory.buffer = ""
              userHistory.prevHour = hour
            }
            setNewRow("user_click_history", userHistory.history, newRow)

            if (is_train) {
              userHistory.buffer += getRowField("click", row)
            }
          } else {
            setNewRow("user_click_history", userHistory.history, newRow)
          }
          
          newRow(pub_id) = row(Common.app_id)
          newRow(pub_domain) = row(Common.app_domain)
          newRow(pub_category) = row(Common.app_category)
          newRow.mkString(",")
      }
    println("number app rows = " + appRows.count)

    val siteRows = rows.filter(row => !Common.is_app(row(Common.site_id)))
      .map {
        row =>
          val newRow = new Array[String](NEW_FIELDS.size)

          for (field <- FIELDS) {
            val index = Common.fieldMap.get(field).get
            val newIndex = newFieldMap.get(field).get
            newRow(newIndex) = row(index)
          }

          newRow(device_id_count) = idMap.value.get(row(Common.device_id)).getOrElse(0) + ""
          newRow(device_ip_count) = ipMap.value.get(row(Common.device_ip)).getOrElse(0) + ""
          val (user, hour) = (Common.defUser(row), row(Common.hour))
          newRow(user_count) = userCountMap.value.get(user).getOrElse(0) + ""
          newRow(smooth_user_hour_count) = userHourCountMap.value.get(user + "-" + hour).getOrElse(0) + ""

          val userHistory = getUserHistory(history, user)
          if (has_id_info(row)) {
            if (userHistory.prevHour != hour) {
              userHistory.history != userHistory.history + userHistory.getLast4Buffer
              userHistory.buffer = ""
              userHistory.prevHour = hour
            }
            setNewRow("user_click_history", userHistory.history, newRow)

            if (is_train) {
              userHistory.buffer += getRowField("click", row)
            }
          } else {
            setNewRow("user_click_history", userHistory.history, newRow)
          }

          newRow(pub_id) = row(Common.site_id)
          newRow(pub_domain) = row(Common.site_domain)
          newRow(pub_category) = row(Common.site_category)

          newRow.mkString(",")
      }

    println("number site rows = " + siteRows.count)

    Common.writeOut(NEW_FIELDS, appRows.collect, dst_app_path)

    Common.writeOut(NEW_FIELDS, siteRows.collect, dst_site_path)
  }

  def has_id_info(row: Array[String]): Boolean = {
    val returnValue = if (row(Common.fieldMap("device_id")) == "a99f214a") false else true
    returnValue
  }

  def getRowField(field: String, row: Array[String]): String = {
    row(Common.fieldMap(field))
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

