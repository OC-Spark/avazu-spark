package org.ocspark.avazu.ensemble.model.appid92f5800b

import org.ocspark.avazu.ensemble.Cvt
import org.ocspark.avazu.Common
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer

class AppId9Cvt extends Cvt {
  val fields = Array[String]("banner_pos","device_model","device_conn_type","C14","C17","C20","C21")
  
  override def convert(src_path : String, dst_path : String, is_train : Boolean, sc : SparkContext){
    val srcData = readSrcData(src_path, sc)
    val label_feats = srcData.map {
      row =>
        val feats = ArrayBuffer[String]()
            for (field <- fields){
                feats.append(tf.indexOf(field+"-"+row(headerMap(field))).toString)
            }
            genHourFeature(row, feats)

        	genCntFeature("device_ip", 100, row, feats)

        	genCntFeature("device_id", 100, row, feats)
        	
        	genUserHourCnt(30, row, feats)
        	
        	genUserHistory2Feature(30, row, feats)

            row(headerMap("id")) + " " + row(headerMap("click")) + " " + feats.mkString(" ")
    }
    val outString = label_feats.collect.mkString("\n")
    Common.writeOut(outString, dst_path)
  }
}
