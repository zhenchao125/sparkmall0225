package com.atguigu.sparkmall0225.offline

import java.util.UUID

import com.alibaba.fastjson.JSON
import com.atguigu.sparkmall0225.common.bean.UserVisitAction
import com.atguigu.sparkmall0225.common.util.ConfigurationUtil
import com.atguigu.sparkmall0225.offline.app.{CategorySessionTop10, CategoryTop10App}
import com.atguigu.sparkmall0225.offline.bean.CategoryCountInfo
import com.atguigu.sparkmall0225.offline.util.Condition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-07-17 15:34
  */
object OfflineApp {
    def main(args: Array[String]): Unit = {
        // 先把 UserAction.. 读出去
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val spark: SparkSession = SparkSession.builder()
            .appName("OfflineApp")
            .master("local[2]")
            .enableHiveSupport()
            .getOrCreate()
        spark.sparkContext.setCheckpointDir("hdfs://hadoop201:9000/sparkmall0225")
        val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionRDD(spark, readCondition)
        userVisitActionRDD.cache()
        userVisitActionRDD.checkpoint()
        val taskId = UUID.randomUUID().toString
        // 需求1:
        val categoryCountTop10: List[CategoryCountInfo] = CategoryTop10App.statCategoryTop10(spark, userVisitActionRDD, taskId)
        // 需求2:
        CategorySessionTop10.statCategoryTop10Session(spark, categoryCountTop10, userVisitActionRDD, taskId)
        
        
    }
    
    /**
      * 从hive中读取用户行为的数据
      *
      * @param spark
      * @param condition
      */
    def readUserVisitActionRDD(spark: SparkSession, condition: Condition): RDD[UserVisitAction] = {
        var sql =
            s"""
               |select
               | v.*
               |from user_visit_action v join user_info u on v.user_id=u.user_id
               |where 1=1
             """.stripMargin
        
        if (isNotEmpty(condition.startDate)) {
            sql += s" and date>='${condition.startDate}'"
        }
        if (isNotEmpty(condition.endDate)) {
            sql += s" and date<='${condition.endDate}'"
        }
        if (condition.startAge > 0) {
            sql += s" and u.age>=${condition.startAge}"
        }
        if (condition.endAge > 0) {
            sql += s" and u.age<=${condition.endAge}"
        }
        import spark.implicits._
        spark.sql("use sparkmall0225")
        spark.sql(sql).as[UserVisitAction].rdd
    }
    
    /**
      * 读取过滤的条件
      *
      * @return
      */
    def readCondition: Condition = {
        val conditionString: String = ConfigurationUtil("conditions.properties").getString("condition.params.json")
        JSON.parseObject(conditionString, classOf[Condition])
    }
}
