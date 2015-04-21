package org.ocspark.avazu.base.converter

import java.security.MessageDigest

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
  //import hashlib, csv, math, os, subprocess
  // "id", "click", "hour", "banner_pos", "device_id", "device_ip", "device_model", "device_conn_type", "C14", "C17", "C20", "C21"
  val fieldMap = Map("id" -> 0, "click" -> 1, "hour" -> 2, "banner_pos" -> 4, "device_id" -> 11, "device_ip" -> 12, "device_model" -> 13, "device_conn_type" -> 15, "C14" -> 16, "C17" -> 19, "C20" -> 22, "C21" -> 23)

  val NR_BINS = 1000000

  def hashstr(input: String) {
    val utfInput = scala.io.Source.fromBytes(input.getBytes(), "URF-8").toString
    val md5Input = MessageDigest.getInstance("MD5").digest(utfInput.getBytes).toString()
    val intInput = Integer.parseInt(md5Input)
    (intInput % (NR_BINS - 1) + 1).toString
  }

  def open_with_first_line_skipped(path: String, skip: Boolean) {
    //    f = open(path)
    //    if not skip:
    //        return f
    //    next(f)
    //    return f
    assert(false, "do not call")
  }

  def split(path: String, nr_thread: Int, has_header: Boolean) {

    /*def open_with_header_witten(path: String, idx : Int, header : String){
        f = open(path + ".__tmp__.{0}".format(idx), "w")
        if not has_header:
            return f 
        f.write(header)
        return f

    def calc_nr_lines_per_thread():
        nr_lines = int(list(subprocess.Popen("wc -l {0}".format(path), shell=True, 
            stdout=subprocess.PIPE).stdout)[0].split()[0])
        if not has_header:
            nr_lines += 1 
        return math.ceil(float(nr_lines)/nr_thread)

    header = open(path).readline()

    nr_lines_per_thread = calc_nr_lines_per_thread()

    idx = 0
    f = open_with_header_witten(path, idx, header)
    for i, line in enumerate(open_with_first_line_skipped(path, has_header), start=1):
        if i%nr_lines_per_thread == 0:
            f.close()
            idx += 1
            f = open_with_header_witten(path, idx, header)
        f.write(line)
    f.close()*/
    assert(false, "do not call")
  }

  def parallel_convert(cvt_path: String, arg_paths: String, nr_thread: Int) {

    /*val workers = Array[String]()
    for (i <- 0 to nr_thread - 1){
        var cmd = "{0}".format(os.path.join(".", cvt_path))
        for path in arg_paths:
            cmd += " {0}".format(path+".__tmp__.{0}".format(i))
        worker = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        workers.append(worker)
    }
    for (worker <- workers){
        worker.communicate()
    }*/
    assert(false, "do not call")
  }

  def cat(path: String, nr_thread: Int) {

    //    if os.path.exists(path):
    //        os.remove(path)
    //    for i in range(nr_thread):
    //        cmd = "cat {svm}.__tmp__.{idx} >> {svm}".format(svm=path, idx=i)
    //        p = subprocess.Popen(cmd, shell=True)
    //        p.communicate()
    assert(false, "do not call")
  }

  def delete(path: String, nr_thread: Int) {

    //    for (i <- range(nr_thread)){
    //        os.remove("{0}.__tmp__.{1}".format(path, i))
    //    }
    assert(false, "do not call")
  }

  def def_user(row: Array[String]): String = {
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
}
