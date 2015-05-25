package org.ocspark.avazu.ensemble.model.appid

import org.ocspark.avazu.ensemble.Cvt
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import org.ocspark.avazu.Common

class AppIdCvt extends Cvt {

  val fields = Array[String]("pub_id","pub_domain","pub_category","banner_pos","device_model","device_conn_type","C14","C17","C20","C21")
  
  override def convert(src_path : String, dst_path : String, is_train : Boolean, sc : SparkContext){
     val srcData = readSrcData(src_path, sc)
    val label_feats = srcData.map {
      row =>
        val feats = ArrayBuffer[String]()
//              println("field = pub_id, row value = " + row(headerMap("pub_id")))
//              println("field = pub_domain, row value = " + row(headerMap("pub_domain")))
//              println("field = pub_category, row value = " + row(headerMap("pub_category")))
           for (field <- fields){
                 feats.append(tf.indexOf(field+"-"+row(headerMap(field))).toString)
            }
        
        	genCntFeature("device_ip", 100, row, feats)

        	genCntFeature("device_id", 20, row, feats)
        	
        	genUserHourCnt(30, row, feats)
        	
        	genUserHistoryFeature(30, row, feats)

            row(headerMap("id")) + " " + row(headerMap("click")) + " " + feats.mkString(" ")		// deviate from original to unify .sp file format - add id in first column
    }
    val outString = label_feats.collect.mkString("\n")
    Common.writeOut(outString, dst_path)
  }
}