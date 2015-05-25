package org.ocspark.avazu.base.converter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import scala.annotation.meta.field
import org.apache.spark.SparkContext
import org.ocspark.avazu.base.util.GenBaseData
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.feature.HashingTF
import org.ocspark.avazu.Common

object Converter2 {

  val NR_BINS = 1000000
  val tf = new HashingTF(NR_BINS)

  val fields = Array("pub_id", "pub_domain", "pub_category", "banner_pos", "device_model", "device_conn_type", "C14", "C17", "C20", "C21")
  val fieldsMap = Map[String, Int]("pub_id" -> 0, "pub_domain" -> 1, "pub_category" -> 2, "banner_pos" -> 3, "device_model" -> 4, "device_conn_type" -> 5, "C14" -> 6, "C17" -> 7, "C20" -> 8, "C21" -> 9)
  val pub_id = 0
  val pub_domain = 1
  val pub_category = 2
  val banner_pos = 3
  val device_model = 4
  val device_conn_type = 5
  val C14 = 6
  val C17 = 7
  val C20 = 8
  val C21 = 9

  def convert(src_path: String, dst_path: String, is_train: Boolean, sc: SparkContext) {
    //    println("src path = " + src_path)
    val srcLines = sc.textFile("hdfs://" + Common.hdfsHost + "/" + src_path, 4)
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map {
        line =>
          val row = line.split(",")
          //          println("row length = " + row.length)
          row
      }
      .cache

    val ids = srcLines.map {
      row =>
        row(0)
    }
    Common.writeOut(Array[String](), ids.collect, dst_path + "_headers")

    val convertedLines = srcLines.map {
      row =>
        val feats = new ArrayBuffer[String]()

        for (field <- fields) {
          feats.append(tf.indexOf(field + "-" + row(GenBaseData.newFieldMap(field))) + "")
        }
        val hour = row(Common.hour)
        val hourString = "hour-" + hour.substring(hour.length - 2)
        feats.append(tf.indexOf(hourString) + "")

        if (row(GenBaseData.device_ip_count).toInt > 1000) {
          feats.append(tf.indexOf("device_ip-" + row(GenBaseData.device_ip)) + "")
        } else {
          feats.append(tf.indexOf("device_ip-less-" + row(GenBaseData.device_ip_count)) + "")
        }

        if (row(GenBaseData.device_id_count).toInt > 1000) {
          feats.append(tf.indexOf("device_id-" + row(GenBaseData.device_id)) + "")
        } else {
          feats.append(tf.indexOf("device_id-less-" + row(GenBaseData.device_id_count)) + "")
        }

        if (row(GenBaseData.smooth_user_hour_count).toInt > 30) {
          feats.append(tf.indexOf("smooth_user_hour_count-0") + "")
        } else {
          feats.append(tf.indexOf("smooth_user_hour_count-" + row(GenBaseData.smooth_user_hour_count)) + "")
        }

        if (row(GenBaseData.user_count).toInt > 30) {
          feats.append(tf.indexOf("user_click_histroy-" + row(GenBaseData.user_count)) + "")
        } else {
          feats.append(tf.indexOf("user_click_histroy-" + row(GenBaseData.user_count) + "-" + getUserClicks(row)) + "")
        }

        row(GenBaseData.id) + " " + row(GenBaseData.click) + " " + feats.toArray.mkString(" ")
    }

    val spLineArray = convertedLines.collect
    println("spLineArray length = " + spLineArray.length)
    Common.writeOut(Array[String](), spLineArray, dst_path)
  }

  def getUserClicks(row: Array[String]) {
    if (row.length == 20) {
      row(GenBaseData.user_click_history)
    } else {
      ""
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Converter 2")
      .setMaster("local[4]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)

    val trSrcPath = args(0) // tr.r{size}.{category}.new.csv
    val vaSrcPath = args(1)
    val trDstPath = args(2) // tr.r{size}.{category}.sp
    val vaDstPath = args(3)
    convert(trSrcPath, trDstPath, true, sc)
    convert(vaSrcPath, vaDstPath, false, sc)

  }
}