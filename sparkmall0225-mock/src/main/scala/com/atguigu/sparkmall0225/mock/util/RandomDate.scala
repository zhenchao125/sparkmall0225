package com.atguigu.sparkmall0225.mock.util

import java.text.SimpleDateFormat
import java.util.Date

object RandomDate {
    def apply(startDate: Date, stopDate: Date, step: Int): RandomDate = {
        val randomDate = new RandomDate
        val avgStepTime = (stopDate.getTime - startDate.getTime) / step
        randomDate.maxStepTime = 4 * avgStepTime
        randomDate.lastDateTIme = startDate.getTime
        randomDate
    }
    
    
    def main(args: Array[String]): Unit = {
        val dateFormate = new SimpleDateFormat("yyyy-MM-dd")
        val from: Date = dateFormate.parse("2019-07-17")
        val to: Date = dateFormate.parse("2019-07-19")
        
        val randomDate = RandomDate(from, to, 10000 )
        println(randomDate.getRandomDate)
        println(randomDate.getRandomDate)
        println(randomDate.getRandomDate)
        println(randomDate.getRandomDate)
        println(randomDate.getRandomDate)
    }
}

class RandomDate {
    // 上次 action 的时间
    var lastDateTIme: Long = _
    // 每次最大的步长时间
    var maxStepTime: Long = _
    
    /**
      * 得到一个随机时间
      * @return
      */
    def getRandomDate: Date = {
        // 这次操作的相比上次的步长
        val timeStep: Long = RandomNumUtil.randomLong(0, maxStepTime)
        lastDateTIme += timeStep
        new Date(lastDateTIme)
    }
}
