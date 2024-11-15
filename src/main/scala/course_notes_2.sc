// Databricks notebook source
// Mutable
var x = 10
x = 20

// COMMAND ----------

// Imutable
val y = 10
y = 20

// COMMAND ----------

def add(firstInput: Int, secondInput: Int): Int = {
  val sum = firstInput + secondInput
  return sum
}

// COMMAND ----------

val addNumbers = add(5,6)

// COMMAND ----------

def encode(n: Int, f: (Int) => Long): Long = {
  val x = n * 10
  f(x)
}

// COMMAND ----------

(x: Int) => {
  x + 100
}

// COMMAND ----------

val higherOrderFunctionTest1 = encode (10, (x: Int) => (x + 100))

// COMMAND ----------

val higherOrderFucntionTest4 = encode (5, _ + 100)

// COMMAND ----------

class Car(mk: String, ml: String, cr: String) {
  val make = mk
  val model = ml
  var color = cr
  def repaint(newColor: String) = {
    color = newColor
  }
}

// COMMAND ----------

val mustang = new Car("Ford", "Mustang", "Red")
val corvette = new Car("GM", "Corvette", "Black")

// COMMAND ----------

// "case class" means that all parameters are immutables by default
case class Message(from: String, to: String, content: String)

// COMMAND ----------

val request = Message("harry", "sam", "discussion")

// COMMAND ----------

def colorToNumber(color: String): Int = {
  val num = color match {
    case "Red" => 1
    case "Blue" => 2
    case "Green" => 3
    case "Yellow" => 4
    case _ => 0
  }
  num
}

// COMMAND ----------

val colorName = "Red"
val colorCode = colorToNumber(colorName)
println(s"The color code fo $colorName is $colorCode")

// COMMAND ----------

def f(x: Int, y: Int, operator: String): Double = {
  operator match {
    case "+" => x + y
    case "-" => x - y
    case "*" => x * y
    case "/" => x / y.toDouble
  }
}

// COMMAND ----------

val sum = f(10, 20, "+")
val product = f(10, 20, "*")