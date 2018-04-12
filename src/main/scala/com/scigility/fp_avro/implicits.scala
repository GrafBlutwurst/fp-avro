package com
package scigility
package fp_avro

import scala.collection.immutable.ListMap
import scalaz.Functor
import scalaz.Applicative
import scalaz.Traverse
import Data._

object implicits {

  implicit val avroTypeFunctor:Functor[AvroType] = new Functor[AvroType] {
    override def map[A,B](fa:AvroType[A])(f:A => B):AvroType[B] = fa match {
      case _:AvroNullType[A] => AvroNullType[B]
      case _:AvroBooleanType[A] => AvroBooleanType[B]
      case _:AvroIntType[A] => AvroIntType[B]
      case _:AvroLongType[A] => AvroLongType[B]
      case _:AvroFloatType[A] => AvroFloatType[B]
      case _:AvroDoubleType[A] => AvroDoubleType[B]
      case _:AvroBytesType[A] => AvroBytesType[B]
      case _:AvroStringType[A] => AvroStringType[B]
      case rec:AvroRecordType[A] => {
        val newFields:ListMap[AvroRecordFieldMetaData, B] = rec.fields.map(fld => fld._1 -> f(fld._2))
        rec.copy[B](fields = newFields)
      }
      case enum:AvroEnumType[A] => enum.copy[B]()
      case AvroArrayType(items) => AvroArrayType[B](f(items))
      case AvroMapType(values) => AvroMapType[B](f(values))
      case AvroUnionType(members) => AvroUnionType[B](members.map(f))
      case fixed:AvroFixedType[A] => fixed.copy[B]()
    }
  }

  implicit def avroValueFunctor[S]:Functor[AvroValue[S, ?]] = new Functor[AvroValue[S, ?]] {
    override def map[A, B](fa:AvroValue[S,A])(f: A => B):AvroValue[S,B] = fa match {
      case AvroNullValue(s) => AvroNullValue[S, B](s) 
      case AvroBooleanValue(s, b) => AvroBooleanValue[S, B](s, b)
      case AvroIntValue(s, i) =>  AvroIntValue[S, B](s, i)
      case AvroLongValue(s, l) => AvroLongValue[S, B](s, l)
      case AvroFloatValue(s, f) => AvroFloatValue[S, B](s, f)
      case AvroDoubleValue(s, d) => AvroDoubleValue[S, B](s, d)
      case AvroBytesValue(s, bytes) => AvroBytesValue[S, B](s, bytes)
      case AvroStringValue(s, value) => AvroStringValue[S, B](s, value)
      case AvroRecordValue(s, fields) => AvroRecordValue[S, B](s, fields.map(kv => kv._1 -> f(kv._2)))
      case AvroEnumValue(s, symbol) => AvroEnumValue[S, B](s, symbol)
      case AvroArrayValue(s, items) => AvroArrayValue[S, B](s, items.map(f))
      case AvroMapValue(s, values) => AvroMapValue[S, B](s, values.map(kv => kv._1 -> f(kv._2)))
      case AvroUnionValue(s, member) => AvroUnionValue[S, B](s, f(member))
      case AvroFixedValue(s, bytes) => AvroFixedValue[S, B](s, bytes)
    }
  }

  implicit def listMapTraverse[K]:Traverse[ListMap[K, ?]] = new Traverse[ListMap[K, ?]] {

    override def traverseImpl[G[_]:Applicative,A,B](fa: ListMap[K, A])(f: A => G[B]): G[ListMap[K, B]] = {
      val applicativeG = Applicative[G]
      fa.foldLeft(applicativeG.pure(ListMap.empty[K,B]))(
        (map, elem) => {
          val mappedElem = f(elem._2)
          val mappedValue = applicativeG.map(mappedElem)( x => elem._1 -> x)
          applicativeG.apply2(map, mappedValue)(_ + _)
        }
      )
    }

  }

}
