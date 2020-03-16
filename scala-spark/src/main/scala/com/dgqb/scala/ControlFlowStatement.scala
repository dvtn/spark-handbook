package com.dgqb.scala

object ControlFlowStatement {
  def main(args: Array[String]): Unit = {
    /**
     * for
     * 1 to 10 这种表示是scala中的操作符操作 1.to(10)
     * 步长
     *
     */
    for(i <- 1 to 10){
      print(i)
    }
    println()
    for(j <- 1 until 10){
      print(j)
    }
    println()
    for(k <- 1.to(10,2)){
      print(k)
    }
    println()

    for( i<- 1 to 3){
      for(j <- 1 to 3){
        println("i="+i+" j="+j)
      }
    }
    println()

    //小九九
    for(i <- 1 until 10; j <- 1 to 10){
      if(i>=j){
        print(i+"x"+j+"="+i*j+"\t")
      }
      if(i==j){
        println()
      }
    }

    for(i <- 1 to 100 if(i%10==0) if(i%4==0)){
      println(i)
    }


    val result = for(i <- 1 to 1000 if(i%100==0)) yield i
    println(result)
    result.foreach(x=>{println(x)})
    result.foreach(println(_))
    result.foreach(println)

    /**
     * if ... else ...
     */
    val a = 100
    if(a < 50){
      println("a is less than 50")

    } else if (a>=50 && a<75){
      println("a is between 50 and 75")
    } else {
      println("a is greater than 75")
    }

  }
}
