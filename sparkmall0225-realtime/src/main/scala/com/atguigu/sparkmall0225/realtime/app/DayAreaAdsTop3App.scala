package com.atguigu.sparkmall0225.realtime.app

import com.atguigu.sparkmall0225.common.util.RedisUtil
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

/**
  * Author lzc
  * Date 2019-07-20 15:44
  */
object DayAreaAdsTop3App {
    def calcDayAreaAdsTop3(ssc: StreamingContext, lastDStream: DStream[(String, Int)]) = {
        
        // 1. 去掉城市维度, 然后再做聚合
        val dayAreaAdsCount: DStream[(String, (String, (String, Int)))] = lastDStream.map {
            case (s, count) => {
                // (s"$day:$area:$city:$adsId", 1)
                val Array(day, area, _, adsId) = s.split(":")
                (s"$day:$area:$adsId", count)
            }
        }.reduceByKey(_ + _).map {
            case (s, count) => {
                val Array(day, area, adsId) = s.split(":")
                (day, (area, (adsId, count)))
            }
        }
        
        // 2. 按照key进行分组, 分出每天的数据
        val dayAreaAdsCountGrouped: DStream[(String, Iterable[(String, (String, Int))])] = dayAreaAdsCount.groupByKey
        val temp1: DStream[(String, Map[String, Iterable[(String, (String, Int))]])] = dayAreaAdsCountGrouped.map {
            case (day, it) => {
                (day, it.groupBy(_._1)) // 按照地区分组
            }
        }
        // 2.1 对结构做调整, 排序, 取前3
        val temp2: DStream[(String, Map[String, List[(String, Int)]])] = temp1.map {
            case (day, map) => {
                val map1: Map[String, List[(String, Int)]] = map.map { // Map(华东 -> List((4,27), (1,26), (5,22))
                    case (area, it) => (area, it.map(_._2).toList.sortBy(-_._2).take(3))
                }
                (day, map1)
            }
        }
        // 3. 写到 mysql
        // 3.1 list转成json字符串
        val dayAreaAdsCountJson = temp2.map {
            case (day, map) => {
                val t = map.map {
                    case (area, adsCountList) => {
                        // 把list转换json字符串
                        //fastjson 对java的数据结构支持的比较, 对scala独有的结构支持的不好
                        import org.json4s.JsonDSL._ // 提供隐式转换
                        val jsonString: String = JsonMethods.compact(JsonMethods.render(adsCountList))
                        (area, jsonString)
                    }
                }
                (day, t)
            }
        }
        // 3.2 写入到redis
        dayAreaAdsCountJson.foreachRDD(rdd => {
            rdd.foreachPartition(it => {
                val client: Jedis = RedisUtil.getJedisClient
                import scala.collection.JavaConversions._
                it.foreach{
                    case (day, map) => {
                        client.hmset("area:ads:top3:" + day, map)
                    }
                }
                client.close()
            })
        })
    }
}

/*
key										value
area:ads:top3:2019-03-23				field				vlue(json格式的字符串)
										华南                {广告1: 1000, 广告2: 500}
										华北                {广告3: 1000, 广告1: 500}
										
数据来自上个需求

RDD[(s"$day:$area:$city:$adsId", 1)] .map
=> RDD[(s"$day:$area:$adsId", 1)] reduceByKey
=> RDD[(s"$day:$area:$adsId", count)] .map
=> RDD[(day, (area, (adsId, count)))]  .groupByKey
=> RDD[(day, Iterable[(area, (adsId, count))])] 对内部的Iterable做groupBy
=> RDD[(day, Map[area, Iterable[(area, (adsId, count))]])] .map


倒推:
=> RDD[(day, Map[area, Iterable[(adsId, count)])])] .map
=> RDD[(day, Map[(area, List[(adsId, count)])])] 排序, 取前3, 变成json格式
=> RDD[(day, Map[(area, "{adsId, count}")])]

=> client.hmset("area:ads:top3:"+day, Map[(area, "{adsId, count}")])
 */