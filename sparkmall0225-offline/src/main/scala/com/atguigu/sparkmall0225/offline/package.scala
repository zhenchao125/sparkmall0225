package com.atguigu.sparkmall0225

package object offline {
    def isEmpty(s:String):Boolean = s == null || s == ""
    def isNotEmpty(s:String) = !isEmpty(s)
}
