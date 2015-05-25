package org.ocspark.avazu.bag.converter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import scala.annotation.meta.field
import org.apache.spark.mllib.feature.HashingTF
import org.ocspark.avazu.base.util.GenBaseData
import org.ocspark.avazu.Common

object Converter6 {

  val NR_BINS = 1000000
  val tf = new HashingTF(NR_BINS)

  def main(args: Array[String]): Unit = {
    val trSrcPath = args(0)
    val vaSrcPath = args(1)
    val trDstPath = args(2)
    val vaDstPath = args(3)

    val sparkConf = new SparkConf().setAppName("Convert6")
      .setMaster("local[4]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)

    convert(trSrcPath, trDstPath, true, sc)
    convert(vaSrcPath, vaDstPath, false, sc)

  }

  val fields = Array[String]("pub_id", "pub_domain", "pub_category", "banner_pos", "device_model", "device_conn_type", "C14", "C17", "C20", "C21")

  def convert(src_path: String, dst_path: String, is_train: Boolean, sc: SparkContext) {
    //      with open(dst_path, "w") as f:
    val lines = sc.textFile("hdfs://" + Common.hdfsHost + src_path, 4)		// change to neutral directory out of base
     .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    
    val output = lines.map {
      line =>
        val row = line.split(",", -1)
//        println("convert6, row length = " + row.length)
        var i = 1
        val w = Math.sqrt(2) / Math.sqrt(15)
        val feats = new ArrayBuffer[String]()

        for (field <- fields) {
//          println("convert6 : field = " + field)
//          println("field index = " + GenData.newFieldMap(field))
          val v = tf.indexOf(field + "-" + row(GenBaseData.newFieldMap(field)))
//          val feat = f"$i:$v:$w%.20f"
          feats.append(f"$i:$v:$w%.20f")
          i += 1
        }
        val hour = row(GenBaseData.newFieldMap("hour"))
        var v = tf.indexOf("hour-" + hour.substring(hour.length - 2))
        feats.append(f"$i:$v:$w%.20f")
        i += 1

        if (row(GenBaseData.newFieldMap("device_ip_count")).toInt > 1000) {
          v = tf.indexOf("device_ip-" + row(GenBaseData.newFieldMap("device_ip")))
          feats.append(f"$i:$v:$w%.20f")
        } else {
          v = tf.indexOf("device_ip-less-" + row(GenBaseData.newFieldMap("device_ip_count")))
          feats.append(f"$i:$v:$w%.20f")
        }
        i += 1

        if (row(GenBaseData.newFieldMap("device_id_count")).toInt > 1000) {
          v = tf.indexOf("device_id-" + row(GenBaseData.newFieldMap("device_id")))
          feats.append(f"$i:$v:$w%.20f")
        } else {
          v = tf.indexOf("device_id-less-" + row(GenBaseData.newFieldMap("device_id_count")))
          feats.append(f"$i:$v:$w%.20f")
        }
        i += 1

        if (row(GenBaseData.newFieldMap("smooth_user_hour_count")).toInt > 30) {
          v = tf.indexOf("smooth_user_hour_count-0")
          feats.append(f"$i:$v:$w%.20f")
        } else {
          v = tf.indexOf("smooth_user_hour_count-" + row(GenBaseData.newFieldMap("smooth_user_hour_count")))
          feats.append(f"$i:$v:$w%.20f")
        }
        i += 1

        if (row(GenBaseData.newFieldMap("user_count")).toInt > 30) {
          v = tf.indexOf("user_click_histroy-" + row(GenBaseData.newFieldMap("user_count")))
          feats.append(f"$i:$v:$w%.20f")
        } else {
          v = tf.indexOf("user_click_histroy-" + row(GenBaseData.newFieldMap("user_count")) + "-" + row(GenBaseData.newFieldMap("user_click_history")))
          feats.append(f"$i:$v:$w%.20f")
        }
        i += 1

        val outputString = "%s %s %s".format(row(GenBaseData.newFieldMap("id")), row(GenBaseData.newFieldMap("click")), feats.mkString(" "))
//        println("outputString = " + outputString)
        "%s %s %s".format(row(GenBaseData.newFieldMap("id")), row(GenBaseData.newFieldMap("click")), feats.mkString(" "))
    }

    Common.writeOut(Array[String](), output.collect, dst_path)

  }

}