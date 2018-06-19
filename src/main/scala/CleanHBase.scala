import java.io.{File, FileInputStream}
import java.util
import com.alibaba.fastjson.parser.Feature
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConversions._

/**
  * Created by Kratos on 2018/5/24.
  */
object CleanHBase {

  def main(args: Array[String]): Unit = {
    val fileInputStream = new FileInputStream(new File("conf/columns.json"))
    val columnsInfo: util.LinkedHashMap[String, JSONObject] = JSON.parseObject(fileInputStream, classOf[util.LinkedHashMap[_, _]], Feature.OrderedField)
    fileInputStream.close()
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(Configuration.Kafka2Spark.HBASE_ZOOKEEPER_QUORUM._1, Configuration.Kafka2Spark.HBASE_ZOOKEEPER_QUORUM._2)
    //设置zookeeper连接端口，默认2181
    hbaseConf.set(Configuration.Kafka2Spark.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT._1, Configuration.Kafka2Spark.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT._2)
    hbaseConf.setInt("hbase.rpc.timeout", Int.MaxValue)
    hbaseConf.setInt("hbase.client.operation.timeout", Int.MaxValue)
    hbaseConf.setInt("hbase.client.scanner.timeout.period", Int.MaxValue)
    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    val sparkSession = SparkSession
      .builder
      .appName("CleanHBase")
//      .master("local")
      .getOrCreate

    hbaseConf.set(TableInputFormat.INPUT_TABLE, "tmp:mainsite_restapi_ele_me_product")
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "tmp:mainsite_restapi_ele_me_product")
    val scan = new Scan()
    val specfoodsKey = Bytes.toBytes("specfoods")
    scan.addFamily(specfoodsKey)
    import org.apache.hadoop.hbase.protobuf.ProtobufUtil
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)
    hbaseConf.set(TableInputFormat.SCAN, scanToString)

    val productInfoRDD = sparkSession.sparkContext.newAPIHadoopRDD(hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).map(line => {
      val row = line._2
      val productId = Bytes.toString(row.getRow)
      val put = new Put(Bytes.toBytes(productId))
      if (!row.getFamilyMap(specfoodsKey).isEmpty) {
        row.getFamilyMap(specfoodsKey).foreach(item => {
          val key = Bytes.toString(item._1)
          val specfoods = Bytes.toString(item._2)
          val values = specfoods.split("  ")
          for (value <- values) {
            if (!value.contains(",")) {
              val v = value.split(":")
              if (v.length == 2) {
                val tmp = columnsInfo.get(v(0))
                if (tmp != null) {
                  val hbaseColumnType = tmp("type")
                  val hbaseColumn = tmp("name").toString
                  hbaseColumnType match {
                    case "String" => {
                      put.addColumn(Bytes.toBytes(hbaseColumn), key.getBytes, Bytes.toBytes(v(1)))
                    }
                    case "int" => {
                      put.addColumn(Bytes.toBytes(hbaseColumn), key.getBytes, Bytes.toBytes(v(1).toInt))
                    }
                    case "float" => {
                      put.addColumn(Bytes.toBytes(hbaseColumn), key.getBytes, Bytes.toBytes(v(1).toFloat))
                    }
                    case "double" => {
                      put.addColumn(Bytes.toBytes(hbaseColumn), key.getBytes, Bytes.toBytes(v(1).toDouble))
                    }
                    case "boolean" => {
                      put.addColumn(Bytes.toBytes(hbaseColumn), key.getBytes, Bytes.toBytes(!v(1).toBoolean))
                    }
                  }
                }
              }
            }
          }
        })
      }
      (new ImmutableBytesWritable, put)
    }).saveAsHadoopDataset(jobConf)
  }
}
