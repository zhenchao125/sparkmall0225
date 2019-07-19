package com.atguigu.sparkmall0225.offline.acc

import com.atguigu.sparkmall0225.common.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

class MapAcc extends AccumulatorV2[UserVisitAction, Map[String, (Long, Long, Long)]] {
    
    // (cid, (clickCount, orderCount, payCount))
    var map: Map[String, (Long, Long, Long)] = Map[String, (Long, Long, Long)]()
    
    // 判断是否为空
    override def isZero: Boolean = map.isEmpty
    
    // copy累加器
    override def copy(): AccumulatorV2[UserVisitAction, Map[String, (Long, Long, Long)]] = {
        val acc = new MapAcc
//        acc.map = Map[String, (Long, Long, Long)]()  // 有误
        acc.map ++= map
        acc
    }
    
    // 重置累加器
    override def reset(): Unit = {
        map = Map[String, (Long, Long, Long)]()
    }
    
    // 累加
    override def add(v: UserVisitAction): Unit = {
        if (v.click_category_id != -1) { // 点击行为
            val (clickCount, orderCount, payCount): (Long, Long, Long) = map.getOrElse(v.click_category_id.toString, (0L, 0L, 0L))
            map += v.click_category_id.toString -> (clickCount + 1, orderCount, payCount)
            
        } else if (v.order_category_ids != null) { // 下单行为   "1,3,2,4"
            val split: Array[String] = v.order_category_ids.split(",") // 这次下单所有的品类
            split.foreach(categoryId => {
                val (clickCount, orderCount, payCount): (Long, Long, Long) = map.getOrElse(categoryId, (0L, 0L, 0L))
                map += categoryId -> (clickCount, orderCount + 1, payCount)
            })
            
        } else if (v.pay_category_ids != null) { // 支付行为
            val split: Array[String] = v.pay_category_ids.split(",") // 这次下单所有的品类
            split.foreach(categoryId => {
                val (clickCount, orderCount, payCount): (Long, Long, Long) = map.getOrElse(categoryId, (0L, 0L, 0L))
                map += categoryId -> (clickCount, orderCount, payCount + 1)
            })
        }
    }
    
    // 分区间累加器的合并
    override def merge(other: AccumulatorV2[UserVisitAction, Map[String, (Long, Long, Long)]]): Unit = {
        /*other match {
            case acc: MapAcc =>
                acc.map.foreach{
                    case (key, (count1, count2, count3)) =>
                        val (c1, c2, c3) = this.map.getOrElse(key, (0, 0, 0))
                        this.map += key -> (count1 + c1, count2 + c2, count3 + c3)
                }
            case _ =>
        }*/
        
        val acc = other.asInstanceOf[MapAcc]
        acc.map.foreach {
            case (key, (count1, count2, count3)) =>
                val (c1, c2, c3) = this.map.getOrElse(key, (0L, 0L, 0L))
                this.map += key -> (count1 + c1, count2 + c2, count3 + c3)
        }
    }
    
    // 返回累加后的结果
    override def value: Map[String, (Long, Long, Long)] = map
}
