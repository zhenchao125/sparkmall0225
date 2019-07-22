package com.atguigu.sparkmall0225.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

case class AdsInfo(ts: Long, area: String, city: String, userId: String, adsId: String) {
    val dayString: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ts))
    val hmStrjing: String = new SimpleDateFormat("HH:mm").format(new Date(ts))
    
    override def toString: String = s"$dayString:$area:$city:$adsId"
}

