import java.text.DecimalFormat

/**
  * Author lzc
  * Date 2019-07-19 15:20
  */
object DecimalDemo {
    def main(args: Array[String]): Unit = {
        val formatter = new DecimalFormat("0.00%")
        /*println(formatter.format(10))
        println(formatter.format(1))
        println(formatter.format(112))
        println(formatter.format(1121))
        println(formatter.format(112111))*/
    
        println(formatter.format(math.Pi))
        println(formatter.format(2.267))
    }
}
