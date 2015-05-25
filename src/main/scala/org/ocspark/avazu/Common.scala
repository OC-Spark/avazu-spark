package org.ocspark.avazu

import java.util.Properties

import scala.beans.BeanInfo

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object Common {
  val app_selector = "85f751fd"
  val id = 0
  val click = 1
  val hour = 2
  val C1 = 3
  val banner_pos = 4
  val site_id = 5
  val site_domain = 6
  val site_category = 7
  val app_id = 8
  val app_domain = 9
  val app_category = 10
  val device_id = 11
  val device_ip = 12
  val device_model = 13
  val device_type = 14
  val device_conn_type = 15
  val C14 = 16
  val C15 = 17
  val C16 = 18
  val C17 = 19
  val C18 = 20
  val C19 = 21
  val C20 = 22
  val C21 = 23

  val fieldMap = Map("id" -> 0, "click" -> 1, "hour" -> 2, "banner_pos" -> 4, "device_id" -> 11, "device_ip" -> 12, "device_model" -> 13, "device_conn_type" -> 15, "C14" -> 16, "C17" -> 19, "C20" -> 22, "C21" -> 23)

  val (hdfsHost) =
    try {
      val prop = new Properties()
      val is = this.getClass().getClassLoader()
        .getResourceAsStream("config.properties");
      prop.load(is)

      (
        prop.getProperty("hdfs.host"))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
    }

  val conf = new Configuration()
  val hdfsCoreSitePath = new Path("core-site.xml")
  conf.addResource(hdfsCoreSitePath)
  val fs = FileSystem.get(conf);

  def defUser(row: Array[String]): String = {
    var user = ""
    if (row(device_id) == "a99f214a") {		// deviceId
      user = "ip-" + row(device_ip) + "-" + row(device_model)
    } else {
      user = "id-" + row(device_id)
    }

    user
  }

  def is_app(site_id: String) : Boolean = {
    val isApp = (site_id == app_selector) 
    isApp
  }

  def has_id_info(device_id: String) : Boolean = {
    val hasId : Boolean = (device_id != "a99f214a")
    hasId
  }

  def add_hour(summand: String, addend: Double) {
    val summandLength = summand.length()
    var date = summand.substring(0, summandLength - 2).toInt
    var hour = summand.substring(summandLength - 2).toInt
    hour += addend.toInt
    if (hour > 23) {
      hour -= 24
      date += 1
    }
    return "{0:6}{1:02d}".format(date, hour)
  }

  def hour_diff(a: String, b: String) {
    var greater = ""
    var smaller = ""
    if (a.toInt > b.toInt) {
      greater = a
      smaller = b
    } else {
      greater = b
      smaller = a
    }

    val greater_day = greater.substring(0, greater.length() - 2).toInt
    val greater_hour = greater.substring(greater.length() - 2).toInt

    val smaller_day = smaller.substring(0, smaller.length() - 2).toInt
    val smaller_hour = smaller.substring(smaller.length() - 2).toInt

    (greater_day - smaller_day) * 24 + (greater_hour - smaller_hour)
  }
   
  def writeOut(header : Array[String], newRows: Array[String], dst_path : String) {
    println("dest file = " + dst_path)
    val path = new Path(dst_path)
    val os = fs.create(path)
    if (header.length > 0){    // if header exists
      os.write((header.mkString(",") + "\n").getBytes())
    }
    for (row <- newRows){
      os.write((row.mkString("") + "\n").getBytes())
    }
    os.close()
  }
  
  def writeOut(newRow: String, dst_path : String) {
    println("dest file = " + dst_path)
    val path = new Path(dst_path)
    val os = fs.create(path)

    os.write(newRow.getBytes())

    os.close()
  }
  
  def createDir(model : String){
    val path = new Path(s"/avazu/ensemble/$model")
    fs.mkdirs(path)
  }
  
  def calcLogLoss(validation : RDD[LabeledPoint], prediction : RDD[Double]) : Double = {
    val numValidation = validation.count()
    println("prediction count = " + prediction.count)
    println("validation count = " + validation.count)
//    val label = validation.map(_.label)
    val predictionAndLabel = prediction.zip(validation.map(_.label))
    val loss = predictionAndLabel.map { case (p, l) =>			// change to log loss
      val err = (l * math.log(p)) + (1 - l) * math.log(1 - p)
      err 
    }.reduce(_ + _)
    val logloss = -1 * (loss / numValidation)
    logloss
  }
  
  def logisticFunction(x : Double) : Double = {
    val p = 1/(1+math.exp(-x))
    p
  }
  
  def inverseLogisticFunction(p : Double) : Double = {
//    print("input p =  " + p + "-")
    val x = math.log(p/(1-p))
//    println("inverted x = " + x)
    x
  }
  
    
  def mergePredictions(itr : Iterable[Double]) : Double = {
    var sum = 0.0
    var total = 0
    itr.foreach {
      d =>
        sum += inverseLogisticFunction(d)
        total += 1
    }
//    println("sum="+sum +"-" + "total=" +total)
    logisticFunction(sum / total)
  }
  
  def genPerEventPrediction(idRDD : RDD[String], prediction : RDD[Double]) : RDD[(String, Double)] = {
        val indexedSiteIdRDD = idRDD.zipWithIndex.map { case (v, i) => i -> v }
    val indexedSitePrediction = prediction.zipWithIndex.map { case (v, i) => i -> v }
    
    val idPrediction : RDD[(String, Double)] = indexedSiteIdRDD.leftOuterJoin(indexedSitePrediction).map {
      case (i, (v1, v2opt)) => (v1, v2opt.getOrElse(0))
    }
    
    idPrediction
  }
  
  def averagePrd(list : Iterable[Double]) : Double = {
    var sum = 0.0
    var total = 0
    for (d <- list){
      sum += inverseLogisticFunction(d)
      total += 1
    }
//    println("sum = " + sum)
    logisticFunction(sum / total)
  }

}
