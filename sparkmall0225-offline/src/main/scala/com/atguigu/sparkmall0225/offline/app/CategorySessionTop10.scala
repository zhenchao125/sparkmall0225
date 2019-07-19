package com.atguigu.sparkmall0225.offline.app

import com.atguigu.sparkmall0225.common.bean.UserVisitAction
import com.atguigu.sparkmall0225.offline.bean.CategoryCountInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-07-19 09:15
  */
object CategorySessionTop10 {
    def statCategoryTop10Session(spark: SparkSession, categoryCountTop10: List[CategoryCountInfo], userVisitActionRDD: RDD[UserVisitAction], taskId: String) ={
        val categoryIdList: List[String] = categoryCountTop10.map(_.categoryId)
        // 1. 先过滤出来top10 品类id的那些用户行为
        val filteredUserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(uva => {
            if(uva.click_category_id != -1){
                categoryIdList.contains(uva.click_category_id.toString)  // 判断点击action是否在top10中
            }else if(uva.order_category_ids != null){
                val cids: Array[String] = uva.order_category_ids.split(",")
                categoryIdList.intersect(cids).nonEmpty  // 判断下单行为是否在top10中
                
            }else if(uva.pay_category_ids != null){
                val cids = uva.pay_category_ids.split(",")
                categoryIdList.intersect(cids).nonEmpty  // 判断支付行为是否在top10中
            }else{
                false
            }
        })
        
        // 2.
        
        
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