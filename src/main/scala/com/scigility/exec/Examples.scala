/*package com.scigility.exec

object Examples {

  /*trait Ordering[A] {

    def compare(x:A, y:A):Int
    def lteq(x:A, y:A) = compare(x, y) <= 0
    //more stuff in the scala.math.Ordering

  }

  def isSorted[A](as: List[A])(implicit ord:Ordering[A]):Boolean = as match {
    case Nil => true
    case _::Nil => true
    case _ => as.sliding(2).map( tpl => (tpl(0), tpl(1)) ).forall( tpl => ord.lteq(tpl._1, tpl._2))
  }


  final case class MyType(sortingField:Int, data:String)

  implicit val myTypeOrd:Ordering[MyType] = new Ordering[MyType]{
    def compare(x:MyType, y:MyType):Int = x.sortingField.compare(y.sortingField)
  }

  def main(args:Array[String]):Unit = {
    println(isSorted(List(MyType(0, "asdf"), MyType(1, "foo"))))
  } */



  /*abstract class Animal {
    def makeSound:String
    def numberOfLegs:Int
  }

  class Dog extends Animal {
    def makeSound:String = "woof"
  }

  class Duck extends Animal {
    def makeSound:String = "quak"
  }*/


  sealed trait Animal
  final case class Dog() extends Animal
  final case class Duck() extends Animal
  final case class Cat() extends Animal


  sealed trait SoundTypeClass[A] {
    def makeSound(a:A):String
  }
  object SoundTypeClass{

    def apply[A](implicit stc: SoundTypeClass[A]) = stc

    def make[A]( f:A => String) = new SoundTypeClass[A] {
      def makeSound(a:A):String = f(a)
    }
  }


  sealed trait LegsTypeClass[A] {
    def nrOfLegs(a:A):Int
  }
  object LegsTypeClass{

    def apply[A](implicit ltc:LegsTypeClass[A]) = ltc

    def make[A](f:A => Int) = new LegsTypeClass[A] {
      def nrOfLegs(a:A):Int = f(a)
    }
  }



  implicit val dogSound:SoundTypeClass[Dog] = SoundTypeClass.make[Dog](_ => "woof")
  implicit val duckSound:SoundTypeClass[Duck] = SoundTypeClass.make[Duck](_ => "quack")

  implicit val dogLegs:LegsTypeClass[Dog] = LegsTypeClass.make[Dog](_ => 4)
  implicit val catLegs:LegsTypeClass[Cat] = LegsTypeClass.make[Cat]( _ => 4)


  def makeAnimalSound[A : SoundTypeClass](a:A) = println(SoundTypeClass[A].makeSound(a))
  def tellNrOfLegs[A : LegsTypeClass](a:A) = println(LegsTypeClass[A].nrOfLegs(a))
  def letAnimalTellNrOfLegs[A : SoundTypeClass : LegsTypeClass](a:A) =
    println( List.fill(LegsTypeClass[A].nrOfLegs(a))(SoundTypeClass[A].makeSound((a))).mkString("", "! ", "!") )


  def main(args:Array[String]):Unit = {
    makeAnimalSound(Duck())
    tellNrOfLegs(Cat())
    letAnimalTellNrOfLegs(Dog())
  }


}
*/