package com.atguigu.sparkmall0225.offline.app

import java.text.DecimalFormat

import com.atguigu.sparkmall0225.common.bean.UserVisitAction
import com.atguigu.sparkmall0225.common.util.JDBCUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-07-19 14:23
  */
object PageConversionApp {
    // 计算页面跳转率
    def calcPageConversionRate(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction], targetPages: String, taskId: String) = {
        // 1. 找到目标页面, 计算出来需要计算的调转流"1->2", "2->3"...
        val pages = targetPages.split(",") // [1,2,3,..7]   7
        val prePages: Array[String] = pages.slice(0, pages.length - 1)
        val postPages: Array[String] = pages.slice(1, pages.length)
        val targetPageFlows = prePages.zip(postPages).map {
            case (p1, p2) => p1 + "->" + p2
        }
        // 2. 先过滤出来目标页面的点击记录
        val targetUserVisiteActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(uva => pages.contains(uva.page_id.toString))
        
        // 3. 计算分母 [page, count]: 每个目标页面的点击次数    reduceByKey countByKey
        val targetPageCountMap: collection.Map[Long, Long] = targetUserVisiteActionRDD.map(uva => (uva.page_id, 1))
            .countByKey()
        
        // 4. 计算分子:  RDD[List["1->2"..]] => RDD["1->2",...] => map["1->2" -> 100, ....]
        val allTargetFlows = targetUserVisiteActionRDD.groupBy(_.session_id).flatMap {
            case (sessionId, uvas) => {
                // UVA
                val sortedUVAList: List[UserVisitAction] = uvas.toList.sortBy(_.action_time)
                val pre = sortedUVAList.slice(0, sortedUVAList.length - 1)
                val post = sortedUVAList.slice(1, sortedUVAList.length)
                
                val prePost: List[(UserVisitAction, UserVisitAction)] = pre.zip(post)
                // 存储的是每个session所有的跳转
                val pageFlows: List[String] = prePost.map {
                    case (uva1, uva2) => uva1.page_id + "->" + uva2.page_id
                }
                // 得到每个Session中所有的目标调转流
                val allTargetFlows: List[String] = pageFlows.filter(flow => targetPageFlows.contains(flow))
                allTargetFlows
            }
        }.map((_, null)).countByKey()
        
        
        // 5. 计算跳转率
        val formatter: DecimalFormat = new DecimalFormat("0.00%")
        val resltRate: collection.Map[String, String] = allTargetFlows.map {
            // "1->2", 1000
            case (flow, count) => {
                val page: Long = flow.split("->")(0).toLong
                (flow, formatter.format(count.toDouble / targetPageCountMap(page)))
            }
        }
        // 6. 写入到mysql
        val args: Iterable[Array[Any]] = resltRate.map {
            case (flow, rate) => Array[Any](taskId, flow, rate)
        }
        JDBCUtil.executeUpdate("truncate page_conversion_rate", null)
        JDBCUtil.executeBatchUpdate("insert into page_conversion_rate values(?, ?, ?)", args)
    }
    
    def main(args: Array[String]): Unit = {
        calcPageConversionRate(null, null, "1,2,3,4,5,6,7", null)
    }
    
}

/*
了解计算哪些页面:
	1,2,3,4,5,6,7
	
	1->2  / 1的页面的总的点击数
	2->3  / 2的页面...
		...
		
	
1. 过滤	RDD[UserVisitAction] , 只剩下目标页面的点击行为

2. 按照页面聚合, 求出来分母

3. 分子:
	假设: arr = Array[1,2,3,4,5,6,7]
	拼一个跳转流  "1->2"  "2->3"
	
	arr1 = 1,2,3,4,5,6
	arr2 = 2,3,4,5,6,7
	
	arr3 = arr1.zip(arr2)
	
	RDD[UserVisitAction] 按照session做分组, 每组内按照时间升序
	
	session1:   1,3,4
	session2:   1,2,3,4
	session3:   1,4,2,3,4
	
	rdd1 = 0- len-2
	rdd1 = 1- len-1
	
	rdd1.zip(rdd2).filter(...)
	
	最终想要的所有的跳转流. 按照调转流做分组, 分别统计...
	
 */