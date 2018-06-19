import java.io.{File, FileInputStream}
import java.net.URI
import java.util
import java.util.Date
import java.util.regex.Pattern

import com.alibaba.fastjson.parser.Feature
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import utils.{AppUtil, SimpleDateFormatThreadSafe, TextUtils}
import scala.collection.GenTraversable
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * Created by Kratos on 2018/6/5.
  */
object CleanDataWithSpark {

  var etlDataInfo: util.List[util.Map[String, Any]] = _
  var valuesInfo: util.Map[String, util.List[String]] = _
  var hbaseConf: Configuration = _
  var jobConf: JobConf = _
  val dateFormat = new SimpleDateFormatThreadSafe("yyyy-MM-dd")
  val date = new Date

  /**
    * 初始化ETL服务参数
    */
  def configService(sparkSession: SparkSession): Unit = {
        val configuration = new Configuration
        val fs = FileSystem.get(new URI("hdfs://mycluster"), configuration)
        var fileInputStream = fs.open(new Path("/user/root/.sparkStaging/" + sparkSession.sparkContext.applicationId + "/etl_data_info.json"))
//    var fileInputStream = new FileInputStream(new File("conf/etl_data_info.json"))
    etlDataInfo = JSON.parseObject(fileInputStream, classOf[util.List[util.Map[String, Any]]], Feature.OrderedField)
    fileInputStream.close
        fileInputStream = fs.open(new Path("/user/root/.sparkStaging/" + sparkSession.sparkContext.applicationId + "/values.json"))
//    fileInputStream = new FileInputStream(new File("conf/values.json"))
    valuesInfo = JSON.parseObject(fileInputStream, classOf[util.Map[String, util.List[String]]], Feature.OrderedField)
    fileInputStream.close
  }

  /**
    * 初始化HBase的读写配置
    */
  def initHBaseConf(): Unit = {
    hbaseConf = HBaseConfiguration.create
    hbaseConf.set(Configuration.Kafka2Spark.HBASE_ZOOKEEPER_QUORUM._1, Configuration.Kafka2Spark.HBASE_ZOOKEEPER_QUORUM._2)
    //设置zookeeper连接端口，默认2181
    hbaseConf.set(Configuration.Kafka2Spark.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT._1, Configuration.Kafka2Spark.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT._2)
    hbaseConf.setInt("hbase.rpc.timeout", Int.MaxValue)
    hbaseConf.setInt("hbase.client.operation.timeout", Int.MaxValue)
    hbaseConf.setInt("hbase.client.scanner.timeout.period", Int.MaxValue)
    jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
  }

  /**
    * 启动清洗服务
    */
  def runService(): Unit = {

    val sparkSession = SparkSession
      .builder
      .appName("CleanDataWithSpark")
//      .master("local")
      .getOrCreate

    configService(sparkSession)

    val trueWords = valuesInfo.get("trueWords")
    val falseWords = valuesInfo.get("falseWords")
    val specialBoolKeys = valuesInfo.get("specialBoolKeys")
    val arrayKeys = valuesInfo.get("arrayKeys")

    val scan = new Scan
    scan.setCaching(1000)
    scan.setCacheBlocks(false)
    val family = Bytes.toBytes("content")
    val dataKey = Bytes.toBytes("Data")
    val pageModelNamesKey = Bytes.toBytes("PageModelNames")
    val propertysKey = Bytes.toBytes("Propertys")
    val downloadTimeKey = Bytes.toBytes("DownloadTime")
    val requestUrlKey = Bytes.toBytes("RequestUrl")
    scan.addColumn(family, dataKey)
    scan.addColumn(family, pageModelNamesKey)
    scan.addColumn(family, propertysKey)
    scan.addColumn(family, downloadTimeKey)
    scan.addColumn(family, requestUrlKey)
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)
    hbaseConf.set(TableInputFormat.SCAN, scanToString)

    for (etlData: util.Map[String, Any] <- etlDataInfo) {
      hbaseConf.set(TableInputFormat.INPUT_TABLE, etlData.get("tableName").toString)
      jobConf.set(TableOutputFormat.OUTPUT_TABLE, etlData.get("outputTable").toString)
      val models = etlData.get("models").asInstanceOf[util.Map[String, Object]].entrySet
      var pattern: Pattern = null
      for (model <- models) {
        val modelKey = model.getKey
        val modelValue = model.getValue.asInstanceOf[JSONObject]
        val rowkeyColumns = modelValue.getJSONArray("rowkeyColumns")
        val columnsJson = modelValue.getJSONObject("columns")
        val rowKeyRegex = modelValue.getString("rowKeyRegex")
        val hasRecordTime = modelValue.containsKey("recordTime")
        if (rowKeyRegex != null) {
          pattern = Pattern.compile(rowKeyRegex)
        }
        val columnsKeys = columnsJson.keySet
        val recordRDD = sparkSession.sparkContext.newAPIHadoopRDD(hbaseConf,
          classOf[TableInputFormat],
          classOf[ImmutableBytesWritable],
          classOf[Result]).filter(line => {
          val row = line._2
          Bytes.toString(row.getValue(family, pageModelNamesKey)) == modelKey
        })
        if (modelValue.containsKey("dataPath")) {
          //清洗列表页类型的数据
          recordRDD.mapPartitions(partition => {
            partition.map(line => {
              val row = line._2
              val puts = ListBuffer[Put]()
              val data = Bytes.toString(row.getValue(family, dataKey))
              val propertys = Bytes.toString(row.getValue(family, propertysKey))
              val dataJson = JSON.parseObject(data)
              val propertysJson = JSON.parseObject(propertys)
              val dataJSONArray = dataJson.getJSONArray(modelValue.getString("dataPath"))
              val downloadTimeBytes = row.getValue(family, downloadTimeKey)
              val downloadTime = Bytes.toLong(downloadTimeBytes)
              date.setTime(downloadTime)
              val dateQualifier = Bytes.toBytes(dateFormat.format(date))
              if (dataJSONArray != null) {
                dataJSONArray.foreach(item => {
                  val itemString = item.toString
                  val itemJson = JSON.parseObject(itemString)
                  var rowKey = ""
                  if (rowKeyRegex == null) {
                    rowkeyColumns.foreach(rowkeyColumn => {
                      rowKey = rowKey + itemJson.getString(rowkeyColumn.toString)
                    })
                    if (TextUtils.isContainChinese(rowKey)) {
                      rowKey = AppUtil.MD5(rowKey)
                    }
                  } else {
                    val matcher = pattern.matcher(itemString)
                    while (matcher.find()) {
                      rowKey = matcher.group(1)
                    }
                  }
                  val put = new Put(Bytes.toBytes(rowKey))
                  if (hasRecordTime) {
                    put.addColumn(Bytes.toBytes("downloadTime"), dateQualifier, downloadTimeBytes)
                  }
                  if (propertysJson != null) {
                    propertysJson.foreach(property => {
                      val propertyValue = property._2.toString
                      if (!propertyValue.isEmpty) {
                        put.addColumn(family, Bytes.toBytes(property._1), Bytes.toBytes(propertyValue))
                      }
                    })
                  }
                  columnsJson.foreach(columnInfo => {
                    val sourceKey = columnInfo._1
                    val columnInfoJson = columnInfo._2.asInstanceOf[JSONObject]
                    val hbaseColumn = columnInfoJson.getString("name")
                    val hbaseColumnType = columnInfoJson.getString("type")
                    val isMulti = columnInfoJson.containsKey("isMulti")
                    var value = itemJson.getString(sourceKey)
                    if (!TextUtils.isBlank(value)) {
                      hbaseColumnType match {
                        case "String" => {
                          put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value))
                          if (isMulti) {
                            put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value))
                          }
                        }
                        case "int" => {
                          if (value.contains(".")) {
                            value = value.split("\\.")(0)
                          }
                          try {
                            put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value.toInt))
                            if (isMulti) {
                              put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value.toInt))
                            }
                          } catch {
                            case ex: NumberFormatException => {
                              put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value))
                              if (isMulti) {
                                put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value))
                              }
                            }
                          }
                        }
                        case "long" => {
                          try {
                            put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value.toLong))
                            if (isMulti) {
                              put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value.toLong))
                            }
                          } catch {
                            case ex: NumberFormatException => {
                              put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value))
                              if (isMulti) {
                                put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value))
                              }
                            }
                          }
                        }
                        case "float" => {
                          try {
                            put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value.toFloat))
                            if (isMulti) {
                              put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value.toFloat))
                            }
                          } catch {
                            case ex: NumberFormatException => {
                              put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value))
                              if (isMulti) {
                                put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value))
                              }
                            }
                          }
                        }
                        case "double" => {
                          try {
                            put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value.toDouble))
                            if (isMulti) {
                              put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value.toDouble))
                            }
                          } catch {
                            case ex: NumberFormatException => {
                              put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value))
                              if (isMulti) {
                                put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value))
                              }
                            }
                          }
                        }
                        case "boolean" => {
                          if (trueWords.contains(value)) {
                            put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(true))
                            if (isMulti) {
                              put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(true))
                            }
                          } else if (falseWords.contains(value)) {
                            if (specialBoolKeys.contains(sourceKey) && value.isEmpty) {
                              put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(true))
                              if (isMulti) {
                                put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(true))
                              }
                            } else {
                              put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(false))
                              if (isMulti) {
                                put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(false))
                              }
                            }
                          } else {
                            if (specialBoolKeys.contains(sourceKey)) {
                              put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(true))
                              if (isMulti) {
                                put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(true))
                              }
                            } else {
                              put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value.toBoolean))
                              if (isMulti) {
                                put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value.toBoolean))
                              }
                            }
                          }
                        }
                        case "Array" => {
                          val sourceValueJSONArray = JSON.parseArray(value)
//                          var str: String = null
                          if (sourceKey == "specfoods") {
                            if (!sourceValueJSONArray.isEmpty) {
                              val subColumns = columnInfoJson.getJSONObject("subColumns")
                              val specfoods = sourceValueJSONArray(0).asInstanceOf[String].split("  ")
                              specfoods.foreach(specfood=>{
                                val v = specfood.split(":")
                                if (v.length == 2) {
                                  val specfoodKey = v(0)
                                  val specfoodValue = v(1)
                                  if (!TextUtils.isBlank(specfoodValue)) {
                                    val subColumnsInfo = subColumns.getJSONObject(specfoodKey)
                                    if (subColumnsInfo != null) {
                                      val subHBbaseColumn = subColumnsInfo.getString("name")
                                      val subHBbaseColumnType = subColumnsInfo.getString("type")
                                      subHBbaseColumnType match {
                                        case "String" => {
                                          put.addColumn(family, Bytes.toBytes(subHBbaseColumn), Bytes.toBytes(specfoodValue))
                                          put.addColumn(Bytes.toBytes(subHBbaseColumn), dateQualifier, Bytes.toBytes(specfoodValue))
                                        }
                                        case "int" => {
                                          put.addColumn(family, Bytes.toBytes(subHBbaseColumn), Bytes.toBytes(specfoodValue.toInt))
                                          put.addColumn(Bytes.toBytes(subHBbaseColumn), dateQualifier, Bytes.toBytes(specfoodValue.toInt))
                                        }
                                        case "float" => {
                                          put.addColumn(family, Bytes.toBytes(subHBbaseColumn), Bytes.toBytes(specfoodValue.toFloat))
                                          put.addColumn(Bytes.toBytes(subHBbaseColumn), dateQualifier, Bytes.toBytes(specfoodValue.toFloat))
                                        }
                                        case "double" => {
                                          put.addColumn(family, Bytes.toBytes(subHBbaseColumn), Bytes.toBytes(specfoodValue.toDouble))
                                          put.addColumn(Bytes.toBytes(subHBbaseColumn), dateQualifier, Bytes.toBytes(specfoodValue.toDouble))
                                        }
                                        case "boolean" => {
                                          put.addColumn(family, Bytes.toBytes(subHBbaseColumn), Bytes.toBytes(!specfoodValue.toBoolean))
                                          put.addColumn(Bytes.toBytes(subHBbaseColumn), dateQualifier, Bytes.toBytes(!specfoodValue.toBoolean))
                                        }
                                      }
                                    }
                                  }
                                }
                              })
                            }
                          }
//                          if (str != null && !str.isEmpty) {
//                            put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(str))
//                            put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(str))
//                          }
                        }
                      }
                    }
                  })
                  puts += put
                })
              }
              puts
            })
          }).flatMap(_.flatten(a => GenTraversable(a))).map(put => {
            (new ImmutableBytesWritable, put)
          }).saveAsHadoopDataset(jobConf)
        } else {
          //清洗详情页类型的数据
          recordRDD.map(line => {
            val row = line._2
            val data = Bytes.toString(row.getValue(family, dataKey))
            val dataJson = JSON.parseObject(data)
            val pageModelNames = Bytes.toString(row.getValue(family, pageModelNamesKey))
            val propertys = Bytes.toString(row.getValue(family, propertysKey))
            val downloadTimeBytes = row.getValue(family, downloadTimeKey)
            val downloadTime = Bytes.toLong(downloadTimeBytes)
            date.setTime(downloadTime)
            val dateQualifier = Bytes.toBytes(dateFormat.format(date))
            val requestUrl = Bytes.toString(row.getValue(family, requestUrlKey))
            var rowKey = ""
            rowkeyColumns.foreach(rowkeyColumn => {
              rowKey = rowKey + dataJson.getString(rowkeyColumn.toString)
            })
            if (TextUtils.isContainChinese(rowKey)) {
              rowKey = AppUtil.MD5(rowKey)
            }
            val put = new Put(Bytes.toBytes(rowKey))
            put.addColumn(Bytes.toBytes("downloadTime"), dateQualifier, downloadTimeBytes)

            columnsJson.foreach(columnInfo => {
              val sourceKey = columnInfo._1
              val columnInfoJson = columnInfo._2.asInstanceOf[JSONObject]
              val hbaseColumn = columnInfoJson.getString("name")
              val hbaseColumnType = columnInfoJson.getString("type")
              var value = dataJson.getString(sourceKey)
              if (!TextUtils.isBlank(value)) {
                hbaseColumnType match {
                  case "String" => {
                    put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value))
                    put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value))
                  }
                  case "int" => {
                    if (value.contains(".")) {
                      value = value.split("\\.")(0)
                    }
                    try {
                      put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value.toInt))
                      put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value.toInt))
                    } catch {
                      case ex: NumberFormatException => {
                        put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value))
                        put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value))
                      }
                    }
                  }
                  case "long" => {
                    try {
                      put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value.toLong))
                      put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value.toLong))
                    } catch {
                      case ex: NumberFormatException => {
                        put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value))
                        put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value))
                      }
                    }
                  }
                  case "float" => {
                    try {
                      put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value.toFloat))
                      put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value.toFloat))
                    } catch {
                      case ex: NumberFormatException => {
                        put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value))
                        put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value))
                      }
                    }
                  }
                  case "double" => {
                    try {
                      put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value.toDouble))
                      put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value.toDouble))
                    } catch {
                      case ex: NumberFormatException => {
                        put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value))
                        put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value))
                      }
                    }
                  }
                  case "boolean" => {
                    if (trueWords.contains(value)) {
                      put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(true))
                      put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(true))
                    } else if (falseWords.contains(value)) {
                      if (specialBoolKeys.contains(sourceKey) && value.isEmpty) {
                        put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(true))
                        put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(true))
                      } else {
                        put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(false))
                        put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(false))
                      }
                    } else {
                      if (specialBoolKeys.contains(sourceKey)) {
                        put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(true))
                        put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(true))
                      } else {
                        put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(value.toBoolean))
                        put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(value.toBoolean))
                      }
                    }
                  }
                  case "Array" => {
                    val sourceValueJSONArray = JSON.parseArray(value)
                    var str = ""
                    if (arrayKeys.contains(sourceKey)) {
                      sourceValueJSONArray.zipWithIndex.foreach(item => {
                        if (item._2 < sourceValueJSONArray.length) {
                          str = str + item._1.toString + " "
                        } else {
                          str = str + item._1.toString
                        }
                      })
                    } else if (sourceKey == "flavors") {
                      sourceValueJSONArray.zipWithIndex.foreach(item => {
                        val tmp = item._1.asInstanceOf[JSONObject].getString("name")
                        if (item._2 < sourceValueJSONArray.length - 1) {
                          str = str + tmp + "/"
                        } else {
                          str = str + tmp
                        }
                      })
                    } else if (sourceKey == "supports") {
                      sourceValueJSONArray.zipWithIndex.foreach(item => {
                        val tmp = item._1.asInstanceOf[JSONObject].getString("description")
                        if (item._2 < sourceValueJSONArray.length - 1) {
                          str = str + tmp + "/"
                        } else {
                          str = str + tmp
                        }
                      })
                    }
                    if (!str.isEmpty) {
                      put.addColumn(Bytes.toBytes(hbaseColumn), dateQualifier, Bytes.toBytes(str))
                      put.addColumn(family, Bytes.toBytes(hbaseColumn), Bytes.toBytes(str))
                    }
                  }
                }
              }
            })
            (new ImmutableBytesWritable, put)
          }).saveAsHadoopDataset(jobConf)
        }
      }
    }
    sparkSession.stop
  }

  def main(args: Array[String]): Unit = {

    initHBaseConf
    runService
  }
}
