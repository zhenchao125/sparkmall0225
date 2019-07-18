package com.atguigu.sparkmall0225.offline.app

import com.atguigu.sparkmall0225.common.bean.UserVisitAction
import com.atguigu.sparkmall0225.offline.acc.MapAcc
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-07-17 16:39
  */
object CategoryTop10App {
    // 统计   计算
    def statCategoryTop10(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction]) = {
        
        val acc = new MapAcc
        spark.sparkContext.register(acc)
        userVisitActionRDD.foreach(action => {
            acc.add(action)
        })
        val sortedList = acc.value.toList.sortBy{
            case (_, (c1, c2, c3)) => (-c1, -c2, -c3)
        }.take(10)
        
        
    }
}
