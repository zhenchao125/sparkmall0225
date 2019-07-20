package com.atguigu.sparkmall0225.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.sparkmall0225.common.util.RedisUtil
import com.atguigu.sparkmall0225.realtime.bean.AdsInfo
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * Author lzc
  * Date 2019-07-20 14:13
  */
object DayAreaCityAdsApp {
    val key = "day:area:city:ads"
    def calcDayAreaCityAdsClickCount(ssc: StreamingContext, filteredAdsInfoDSteam: DStream[AdsInfo]) = {
        
        // 1. 统计结果
        val adsInfoOne: DStream[(String, Int)] = filteredAdsInfoDSteam.map {
            case AdsInfo(ts, area, city, _, adsId) => {
                val day: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
                (s"$day:$area:$city:$adsId", 1)
            }
        }
        val resltDStream: DStream[(String, Int)] = adsInfoOne.updateStateByKey((seq: Seq[Int], option: Option[Int]) => {
            Some(seq.sum + option.getOrElse(0))
        })
        
        // 2. 写到redis
        resltDStream.foreachRDD(rdd => {
            rdd.foreachPartition(it => {
                // 建立redis连接
                val client: Jedis = RedisUtil.getJedisClient
                val list: List[(String, Int)] = it.toList
                list.foreach{
                    case (field, value) => {
                        client.hset(key, field, value.toString)
                    }
                }
                client.close()
            })
        })
        resltDStream  //给后面需求使用
        
    }
}
/*
每天各地区各城市各广告的点击流量实时统计
day:area:city:ads

redis key value 设计

key								value(hash)
"day:area:city:ads"				field						value
								2019-07-20:华北:北京:1		10
								
----

DStream[AdsInfo]
=> DSteam[("2019-07-20:华北:北京:1", 1)]  .reduceByKey

=> DSteam[("2019-07-20:华北:北京:1", 100)]

=> 向redis写入
 */