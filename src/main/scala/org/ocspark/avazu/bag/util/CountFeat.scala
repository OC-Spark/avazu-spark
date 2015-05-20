package org.ocspark.avazu.bag.util

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.annotation.meta.field

import org.apache.spark.sql.Row;
// Import Spark SQL data types
import org.apache.spark.sql.types.{ StructType, StructField, StringType };

object CountFeat {

  def main(args: Array[String]): Unit = {
    val tr_path = args(0)
    val va_path = args(1)
    val min_occur = args(2).toInt
//    val cnt_path = args(3)
    
    val conf = new SparkConf().setAppName("Count Feat").setMaster("local[4]")
    val sc = new SparkContext(conf)
    
  }
  
  def countFeatures(tr_path : String, va_path : String, min_occur : Int, sc : SparkContext) : Map[String, Map[String, Int]] = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //	case class Csv(id : String,click : String,hour : String,C1 : String,banner_pos : String,site_id : String,site_domain : String,site_category : String,app_id : String,app_domain : String,app_category : String,
    //	    device_id : String,device_ip : String,device_model : String,device_type : String,device_conn_type : String,C14 : String,C15 : String,C16 : String,C17 : String,C18 : String,C19 : String,C20 : String,C21 : String)

    val schemaString = "id click hour C1 banner_pos site_id site_domain site_category app_id app_domain app_category device_id device_ip device_model device_type device_conn_type C14 C15 C16 C17 C18 C19 C20 C21"

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val tr = sc.textFile(tr_path)
    val va = sc.textFile(va_path)

    val trva = tr.union(va).map(_.split(",")).map{
      p => 
//        val len = p.length
//        println("p len = " + len)
        Row(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11), p(12), p(12), p(13), p(14), p(15), p(16), p(17), p(18), p(19), p(1), p(20), p(21), p(22), p(23))
    }

    val csvDataFrame = sqlContext.createDataFrame(trva, schema)

    val group_counts = collection.mutable.HashMap.empty[String, Map[String, Int]]

    var nr_feats = 0

    for (field <- csvDataFrame.columns) {
//      println("field: " + field)
      val cnts = csvDataFrame.groupBy(field).count.map(row => (row(0).toString, row(1).toString)).collect
      val cntMap = collection.mutable.HashMap[String, Int]()
      cnts.foreach{
        t => 
          if (t._2.toInt > min_occur) {
            cntMap.put(t._1, t._2.toInt)
//            println("key: " + t._1 + " count:" + t._2)
          }
      }
      group_counts.put(field, cntMap.toMap)
      nr_feats += cnts.size
    }

    group_counts.toMap
    //	with open(args['cnt_path'], 'wb') as fh:
    //		pickle.dump(group_counts, fh) 
  }

}