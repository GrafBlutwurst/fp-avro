package com
package scigility
package fp_avro

import scala.collection.immutable.ListMap
import scalaz.Functor
import Data._

object implicits {

  implicit val avroTypeFunctor:Functor[AvroType] = new Functor[AvroType] {
    override def map[A,B](fa:AvroType[A])(f:A => B):AvroType[B] = fa match {
      case _:AvroNull[A] => AvroNull[B]
      case _:AvroBoolean[A] => AvroBoolean[B]
      case _:AvroInt[A] => AvroInt[B]
      case _:AvroLong[A] => AvroLong[B]
      case _:AvroFloat[A] => AvroFloat[B]
      case _:AvroDouble[A] => AvroDouble[B]
      case _:AvroBytes[A] => AvroBytes[B]
      case _:AvroString[A] => AvroString[B]
      case rec:AvroRecord[A] => {
        val newFields:ListMap[AvroRecordFieldMetaData, B] = rec.fields.map(fld => fld._1 -> f(fld._2))
        rec.copy[B](fields = newFields)
      }
      case enum:AvroEnum[A] => enum.copy[B]()
      case AvroArray(items) => AvroArray[B](f(items))
      case AvroMap(values) => AvroMap[B](f(values))
      case AvroUnion(members) => AvroUnion[B](members.map(f))
      case fixed:AvroFixed[A] => fixed.copy[B]()
     
    }
  }

}
