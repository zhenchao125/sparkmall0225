package com.atguigu.sparkmall0225.realtime

import com.atguigu.sparkmall0225.common.util.MyKafkaUtil
import com.atguigu.sparkmall0225.realtime.app.{BlackListApp, DayAreaCityAdsApp}
import com.atguigu.sparkmall0225.realtime.bean.AdsInfo
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealtimeApp {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        // 1. 创建 SparkConf 对象
        val conf: SparkConf = new SparkConf()
            .setAppName("RealTimeApp")
            .setMaster("local[*]")
        // 2. 创建 SparkContext 对象
        val sc = new SparkContext(conf)
        // 3. 创建 StreamingContext
        val ssc = new StreamingContext(sc, Seconds(2))
//        ssc.checkpoint("hdfs://hadoop201:9000/sparkmall0225")
        sc.setCheckpointDir("hdfs://hadoop201:9000/sparkmall0225")
        // 4. 得到 DStream
        val recordDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getDStream(ssc, "ads_log0225")
    
        val adsInfoDStream: DStream[AdsInfo] = recordDStream.map(record => {
            val msg: String = record.value()  // 取出其中的value
            val arr: Array[String] = msg.split(",")   // 切割并封装到 AdsInfo中
            AdsInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
        })
        
        
        // 需求1:
        val filteredAdsInfoDSteam: DStream[AdsInfo] = BlackListApp.filterBlackList(ssc, adsInfoDStream)
//        BlackListApp.checkUserToBlackList(ssc, filteredAdsInfoDSteam)
        
        // 需求2:
        DayAreaCityAdsApp.calcDayAreaCityAdsClickCount(ssc, filteredAdsInfoDSteam)
        
        ssc.start()
        ssc.awaitTermination()
    
    }
}
/*

1. 先把数据从kafka来消费

2. 再去处理黑名单
 */