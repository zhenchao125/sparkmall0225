package com.atguigu.sparkmall0225.realtime.app

import java.util

import com.atguigu.sparkmall0225.common.util.RedisUtil
import com.atguigu.sparkmall0225.realtime.bean.AdsInfo
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
  * Author lzc
  * Date 2019-07-20 10:09
  */
object BlackListApp {
    val dayUserAdsCount = "day:userId:adsId"
    val blackList = "blacklist"
    
    // 检测用户是否要加入到黑名单
    def checkUserToBlackList(ssc: StreamingContext, adsInfoDStream: DStream[AdsInfo]) = {
        
        // redis连接0 ×
        adsInfoDStream.foreachRDD(rdd => {
            // redis连接1  ×
            rdd.foreachPartition(adsInfoIt => {
                // redis连接2 √
                //1.统计每天每用户每广告的点击量
                val client: Jedis = RedisUtil.getJedisClient
                adsInfoIt.foreach(adsInfo => {
                    println(adsInfo.userId)
                    val field = s"${adsInfo.dayString}:${adsInfo.userId}:${adsInfo.adsId}"
                    client.hincrBy(dayUserAdsCount, field, 1)
                    
                    //2. 加入黑名单
                    val clickCount: String = client.hget(dayUserAdsCount, field)
                    if (clickCount.toLong > 100000) {
                        client.sadd(blackList, adsInfo.userId)
                    }
                })
                
                client.close() // 不是真正的关闭连接, 而是把这个连接交个连接池管理
            })
        })
        
        
    }
    
    // 过滤黑名单
    def filterBlackList(ssc: StreamingContext, adsInfoDStream: DStream[AdsInfo]) = {
        
        // 黑名单在实时更新, 所以获取黑名单也应该实时的获取, 一个周期获取一次黑名单
        adsInfoDStream.transform(rdd => {
            val client: Jedis = RedisUtil.getJedisClient
            val blackBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(client.smembers(blackList))
            client.close()
            rdd.filter(adsInfo => {
                val blackListIds: util.Set[String] = blackBC.value
                !blackListIds.contains(adsInfo.userId)
            })
        })
        
    }
}

/*
1. 离线需求会比较复杂, 计算比较耗时

2. 实时需求一般比较简单

----

统计点击广告黑名单:
	当某个用户一天内点击某个广告的次数超过一个范围(100)把这个用户加入黑名单
	
	1. 统计每个用户每天对每个广告的点击量
		有状态?  X
		redis 中 有自增  hash
		
		key										value
		
		"day:userId:adsId:" + 年月日			field			vaue
												userId:adsId    1
												
		----
		
		key										value
		
		"day:userId:adsId"						field						vaue
												dayString:userId:adsId    	1
	
	2. 超过范围的加入黑名单
		
		获取到点击量, 然后判断是否超过阈值
		
		"101", "102", "103"
		
		key						value
		
		"blacklist"				set
		
 */
