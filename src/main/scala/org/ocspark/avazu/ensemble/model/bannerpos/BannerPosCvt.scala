package org.ocspark.avazu.ensemble.model.bannerpos

import org.ocspark.avazu.ensemble.Cvt
import org.ocspark.avazu.Common
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer

class BannerPosCvt extends Cvt {

  val fields = Array[String]("pub_id", "pub_domain", "pub_category", "device_model", "device_conn_type", "C14", "C17", "C20", "C21")

  override def convert(src_path: String, dst_path: String, is_train: Boolean, sc: SparkContext) {
    val srcData = readSrcData(src_path, sc)
    val label_feats = srcData.map {
      row =>
        val feats = ArrayBuffer[String]()
        for (field <- fields) {
          feats.append(tf.indexOf(field + "-" + row(headerMap(field))).toString)
        }
        genHourFeature(row, feats)

        genCntFeature("device_ip", 1000, row, feats)

        genCntFeature("device_id", 1000, row, feats)

        genUserHourCnt(30, row, feats)

        genUserHistoryFeature(30, row, feats)

        genSplCntFeature(20, row, feats)

        feats.append(tf.indexOf("user_count-" + row(headerMap("user_cnt"))).toString)

        row(headerMap("id")) + " " + row(headerMap("click")) + " " + feats.mkString(" ")

    }
    val outString = label_feats.collect.mkString("\n")
    Common.writeOut(outString, dst_path)
  }

}