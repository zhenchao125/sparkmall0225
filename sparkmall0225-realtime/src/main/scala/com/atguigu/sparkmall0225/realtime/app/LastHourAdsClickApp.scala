package com.atguigu.sparkmall0225.realtime.app

import com.atguigu.sparkmall0225.common.util.RedisUtil
import com.atguigu.sparkmall0225.realtime.bean.AdsInfo
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
  * Author lzc
  * Date 2019-07-22 08:43
  */
object LastHourAdsClickApp {
    def calcLastHourAdsClick(ssc: StreamingContext, filteredAdsInfoDSteam: DStream[AdsInfo]) ={
        val dSteamWithWindow: DStream[AdsInfo] = filteredAdsInfoDSteam.window(Minutes(60), Seconds(6))
        
        val adsIdHourMinutesCount: DStream[(String, (String, Int))] = dSteamWithWindow.map(adsInfo => {
            ((adsInfo.adsId, adsInfo.hmStrjing), 1)
        }).reduceByKey(_ + _).map {
            case ((adsId, hourMinute), count) => (adsId, (hourMinute, count))
        }
    
        val adsHMCountJson = adsIdHourMinutesCount.groupByKey().map{
            case (adsId, hmCountIt) => {
                import org.json4s.JsonDSL._
                (adsId, JsonMethods.compact(JsonMethods.render(hmCountIt.toList.sortBy(_._1))))
            }
        }
        
        
        // 写入到redis
    
        adsHMCountJson.foreachRDD(rdd => {
            /*rdd.foreachPartition(it => {
                val client: Jedis = RedisUtil.getJedisClient
                
                it.foreach{
                    case (adsId, json) => {
                        client.hset("last:hour:clcik", adsId, json)
                    }
                }
                client.close()
            })*/
            
            val client: Jedis = RedisUtil.getJedisClient
            val map: Map[String, String] = rdd.collect.toMap
            import scala.collection.JavaConversions._
            client.hmset("last:hour:clcik", map)
            client.close()
            
        })
    }
}
/*
每6秒钟刷新一下最近 1 小时广告点击量实时统计, 显示每分钟的点击量

windows  1小时窗口的长度   窗口的步长: 6s

统计好的数据:  to redis

redis的数据类型:

key						value
"last:hour:clcik"       hash
						field			value
						
						adsId			{"10:01": 10, "10:02":20, ...}


=> (adsId, 小时分钟)  .map
=> ((adsId, 小时分钟), 1)) .reduceByKey
=> ((adsId, 小时分钟), clickCount))  .map
=> (adsId, (小时分钟, clickCount)))	.groupByKey
=> Map[adsId, List[(小时分钟, clickCount)]]
=> Map[adsId, jsonString]
client.hmset("last:hour:clcik" , Map[adsId, jsonString])
 */