package com.atguigu.sparkmall0225.offline.app

import com.atguigu.sparkmall0225.common.bean.UserVisitAction
import com.atguigu.sparkmall0225.common.util.JDBCUtil
import com.atguigu.sparkmall0225.offline.acc.MapAcc
import com.atguigu.sparkmall0225.offline.bean.CategoryCountInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Author lzc
  * Date 2019-07-17 16:39
  */
object CategoryTop10App {
    // 统计   计算
    def statCategoryTop10(spark: SparkSession, userVisitActionRDD: RDD[UserVisitAction], taskId: String) = {
        val acc: MapAcc = new MapAcc
        spark.sparkContext.register(acc)
        userVisitActionRDD.foreach(action => {
            acc.add(action)
        })
        val sortedList = acc.value.toList.sortBy { // 降序排列
            case (_, (c1, c2, c3)) => (-c1, -c2, -c3)
        }.take(10)
        // 2. 把计算到的数据封装到样例对象中 List[样例类]
        val Top10CategoryCountInfo: List[CategoryCountInfo] = sortedList.map {
            case (cid, (c1, c2, c3)) => CategoryCountInfo(taskId, cid, c1, c2, c3)
        }
        // 3. 写入mysql中
        val sql = "insert into category_top10 values(?, ?, ?, ?, ?)"
        val args: List[Array[Any]] = Top10CategoryCountInfo.map(cci => {
            Array[Any](cci.taskId, cci.categoryId, cci.clickCount, cci.orderCount, cci.payCount)
        })
        JDBCUtil.executeBatchUpdate(sql, args)
        Top10CategoryCountInfo
    }
}
/*

写入mysql:
    1.  遍历list 然后去写
    
    
    2.  转换df, 然后使用 df.write.jdbc(....)
 */