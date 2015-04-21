package org.ocspark.avazu.base.converter


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.util.Properties

object Converter2 {
    val (hdfsHost) =
    try {
      val prop = new Properties()
      val is = this.getClass().getClassLoader()
        .getResourceAsStream("config.properties");
      prop.load(is)

      (
        prop.getProperty("hdfs.host")
        )
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
    }
    

  val conf = new Configuration()
//  val hdfsCoreSitePath = new Path("core-site.xml")
//  conf.addResource(hdfsCoreSitePath)
  val fs = FileSystem.get(conf);
  

val fields = Array("pub_id","pub_domain","pub_category","banner_pos","device_model","device_conn_type","C14","C17","C20","C21")

def convert(src_path : String, dst_path : String, is_train : Boolean){
    /*with open(dst_path, 'w') as f:
        for row in csv.DictReader(open(src_path)):
            
            feats = []

            for field in fields:
                feats.append(hashstr(field+'-'+row[field]))
            feats.append(hashstr('hour-'+row['hour'][-2:]))

            if int(row['device_ip_count']) > 1000:
                feats.append(hashstr('device_ip-'+row['device_ip']))
            else:
                feats.append(hashstr('device_ip-less-'+row['device_ip_count']))

            if int(row['device_id_count']) > 1000:
                feats.append(hashstr('device_id-'+row['device_id']))
            else:
                feats.append(hashstr('device_id-less-'+row['device_id_count']))

            if int(row['smooth_user_hour_count']) > 30:
                feats.append(hashstr('smooth_user_hour_count-0'))
            else:
                feats.append(hashstr('smooth_user_hour_count-'+row['smooth_user_hour_count']))

            if int(row['user_count']) > 30:
                feats.append(hashstr('user_click_histroy-'+row['user_count']))
            else:
                feats.append(hashstr('user_click_histroy-'+row['user_count']+'-'+row['user_click_histroy']))

            f.write('{0} {1} {2}\n'.format(row['id'], row['click'], ' '.join(feats)))*/
}


  def main(args: Array[String]): Unit = {
//      if (args.size == 1){
//            args.append("-h")
//      }
    val trSrcPath = args(0)
    val trDstPath = args(1)
    val vaSrcPath = args(2)
    val vaDstPath = args(3)
      convert(trSrcPath, trDstPath, true)
      convert(vaSrcPath, vaDstPath, false)

  }
  
}