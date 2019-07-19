package com.atguigu.sparkmall0225.offline.app

import java.util.Properties

import com.atguigu.sparkmall0225.common.bean.UserVisitAction
import com.atguigu.sparkmall0225.offline.bean.{CategoryCountInfo, CategorySession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Author lzc
  * Date 2019-07-19 09:15
  */
object CategorySessionTop10 {
    def statCategoryTop10Session(spark: SparkSession, categoryCountTop10: List[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction], taskId: String) = {
        // 1. top10的品类id
        val categoryIdList: List[String] = categoryCountTop10.map(_.categoryId)
        // 2. 先过滤出来top10 品类id的那些用户行为
        val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(uva => {
            if (uva.click_category_id != -1) {
                categoryIdList.contains(uva.click_category_id.toString) // 判断点击action是否在top10中
            } else if (uva.order_category_ids != null) {
                val cids: Array[String] = uva.order_category_ids.split(",")
                categoryIdList.intersect(cids).nonEmpty // 判断下单行为是否在top10中
                
            } else if (uva.pay_category_ids != null) {
                val cids = uva.pay_category_ids.split(",")
                categoryIdList.intersect(cids).nonEmpty // 判断支付行为是否在top10中
            } else {
                false
            }
        })
        
        // 3. 统计每个品类的top10 session
        // 3.1 => RDD[(categoryId, sessionId)] .map
        //=> RDD[(categoryId, sessionId), 1)]
        val categorySessionOne: RDD[((String, String), Int)] = filteredUserVisitActionRDD.flatMap(uva => {
            //uva  有可能是点击, 也可能是下单, 支付
            if (uva.click_category_id != -1) {
                Array(((uva.click_category_id + "", uva.session_id), 1)) // flatMap要求必须返回集合, 所以这个时候即使只有一个, 也应该放在集合中
            } else if (uva.order_category_ids != null) {
                val cids: Array[String] = uva.order_category_ids.split(",") // 1    (1,2,3)
                categoryIdList.intersect(cids).map(cid => {
                    ((cid, uva.session_id), 1)
                })
            } else {
                val cids: Array[String] = uva.pay_category_ids.split(",") // 1    (1,2,3)
                categoryIdList.intersect(cids).map(cid => {
                    ((cid, uva.session_id), 1)
                })
            }
        })
        
         // 3.2 聚合  => RDD[(categoryId, sessionId), count)]  => RDD[(categoryId, (sessionId, count))]
        val categorySessionCount: RDD[(String, (String, Int))] = categorySessionOne.reduceByKey(_ + _).map {
            case ((cid, sid), count) => (cid, (sid, count))
        }
        // 3.3 分组, 降序, 取前10
        val categorySessionContTop10: RDD[(String, List[(String, Int)])] = categorySessionCount.groupByKey().map {
            case (cid, sessionCountIt) => {
                (cid, sessionCountIt.toList.sortBy(_._2)(Ordering.Int.reverse).take(10))
            }
        }
        // 4. 写入到mysql
        // 4.1 数据封装到样例类中
        import spark.implicits._
        val resultRDD: RDD[CategorySession] = categorySessionContTop10.flatMap {
            case (cid, sessionCountList) => {
                sessionCountList.map {
                    case (sid, count) => CategorySession(taskId, cid, sid, count)
                }
            }
        }
        // 4.2 使用df写入到mysql中
        val props = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "aaa")
        resultRDD.toDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://hadoop201:3306/sparkmall0225", "category_top10_session_count", props)
    }
}

/*
需求2: Top10热门品类中每个品类的 Top10 活跃 Session 统计

=> RDD[(categoryId, sessionId)] .map
=> RDD[(categoryId, sessionId), 1)]  .reduceByKey
=> RDD[(categoryId, sessionId), count)]    .map
=> RDD[(categoryId, (sessionId, count))]   .groupByKey
=> RDD[(categoryId, List[(sessionId, count)])]
 */