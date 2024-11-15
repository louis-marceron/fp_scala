import scala.collection.BitSet.empty.to

def f(x: Int): Int = x * 2

def g(x: Int): Int = x + 2

def h(x: Int): Int = f(g(x))

val input = 4
println(s"g($input) = ${g(input)}")
println(s"g(f($input)) = ${g(f(input))}")
println(s"h($input) = ${h(input)}")

// Static typing
val name: String = "Scala"
val age: Int = 25

// val language: Int = "Scala"

// Operators
val x = 10
val y = 20
val z = x + y

trait Shape {
  def area(): Int
}

/* class Square(length: Int) extends Shape {
  def area = leng
}*/


// Tupples
val twoElements = ("10", true)
val threeElements = ("10", "harry", true)
// Collections
val array = Array(1, 2, 3, 4, 5)
array(0) = 10

val list = List(1, 2, 3, 4, 5)
val set = Set(1, 2, 3, 4, 5)


val map = Map("apple" -> 1, "pear" -> 2, "banana" -> 3)
val apple = map("apple")

val list2 = list.map(x => x * 2)
val list2Reduced = list.map(_ * 2)

val line = "Scala is a programming language"
val SingleSpace = " "
val words = line.split(SingleSpace)
val arrayOfListOfChar = words.map(_.toList)

val arrayOfChar = words.flatMap(_.toList)

words.foreach(println)