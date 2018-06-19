/**
  * Created by daiquanyi on 2017/2/9.
  */
object Configuration {

  object Kafka2Spark {

    /**
      * 开发环境ZOOKEEPER_CONNECT
      */
//    val ZOOKEEPER_CONNECT = ("zookeeper.connect",
//      "192.168.10.91:2181," +
//        "192.168.10.92:2181," +
//        "192.168.10.93:2181," +
//        "192.168.10.94:2181," +
//        "192.168.10.95:2181")

    //    /**
    //      * 生产环境ZOOKEEPER_CONNECT
    //      */
        val ZOOKEEPER_CONNECT = ("zookeeper.connect",
          "192.168.10.88:2181," +
            "192.168.10.89:2181," +
            "192.168.10.90:2181")

    /**
      * 开发环境METADATA_BROKER_LIST
      */
    val METADATA_BROKER_LIST = ("metadata.broker.list",
      "192.168.10.91:9092," +
        "192.168.10.92:9092," +
        "192.168.10.93:9092," +
        "192.168.10.94:9092," +
        "192.168.10.95:9092")

    //    /**
    //      * 生产环境METADATA_BROKER_LIST
    //      */
    //    val METADATA_BROKER_LIST = ("metadata.broker.list",
    //      "192.168.10.91:9092," +
    //        "192.168.10.92:9092," +
    //        "192.168.10.93:9092")

    /**
      * 生产环境HBASE_ZOOKEEPER_QUORUM
      */
    val HBASE_ZOOKEEPER_QUORUM = ("hbase.zookeeper.quorum",
      "192.168.10.81," +
        "192.168.10.82," +
        "192.168.10.83," +
        "192.168.10.84," +
        "192.168.10.85,")

//    /**
//      * 开发环境HBASE_ZOOKEEPER_QUORUM
//      */
//    val HBASE_ZOOKEEPER_QUORUM = ("hbase.zookeeper.quorum",
//      "Tbybcloud91," +
//        "Tbybcloud92," +
//        "Tbybcloud93,")

    val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = ("hbase.zookeeper.property.clientPort", "2181")

    val TOPICS =
      "10jqka-com-cn," +
        "baidu-com," +
        "ele-me," +
        "jd-com," +
        "jin10-com," +
        "lagou-com," +
        "meituan-com," +
        "tianyancha-com," +
        "tmall-com," +
        "tootoo-cn," +
        "xueqiu-com," +
        "yhd-com," +
        "kanzhun-com," +
        "kaoshi100-cn," + 
        "3-cn"

    //    val TOPICS = "ele-me"

    val GROUP_ID = ("group.id", "daiquanyi123")

    val AUTO_OFFSET_RESET = ("auto.offset.reset", "smallest")

    val SERIALIZER_CLASS = ("serializer.class", "kafka.serializer.StringEncoder")

    val AUTO_COMMIT_INTERVALMS = ("auto.commit.interval.ms", "1000")

    val PARTITION_ASSIGNMENT_STRATEGY = ("partition.assignment.strategy", "roundrobin")

  }

}
