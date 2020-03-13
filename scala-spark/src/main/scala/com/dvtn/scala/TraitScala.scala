package com.dvtn.scala

/**
 * Trait: 物质特性，第一个关键字用extends, 后面都用with
 */

trait Speak{
  def speak(name:String) = {
    println(name + " is speaking")
  }
}

trait Listen{
  def listen(name:String) = {
    println(name + " is listening")
  }
}

trait Write {
  def write(name:String) = {
    println(name+" is writing")
  }
}

class People() extends Speak with Listen with Write {

}

trait isEqual{
  def isEqu(p:Any):Boolean
  def isNotEqu(p:Any):Boolean = {
    !isEqu(p)
  }
}

class Point(xx:Int, yy:Int) extends isEqual {
  val x = xx
  val y = yy

  def isEqu(p:Any): Boolean = {
    p.isInstanceOf[Point] && p.asInstanceOf[Point].x==this.x
  }
}

object TraitScala {
  def main(args: Array[String]): Unit = {
    val p1 = new People()
    p1.speak("风清扬")
    p1.listen("独孤求败")
    p1.write("令狐冲")

    val p2 = new Point(1,2)
    val p3 = new Point(1,3)

    println(p2.isEqu(p3))
    println(p2.isNotEqu(p3))

  }
}
