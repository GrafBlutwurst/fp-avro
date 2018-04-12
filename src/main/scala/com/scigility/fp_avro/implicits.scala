package com
package scigility
package fp_avro

import scala.collection.immutable.ListMap
import scalaz.Functor
import scalaz.Applicative
import scalaz.Traverse
import Data._
import scalaz._
import Scalaz._

object implicits {
  //FIXME: change to traverse
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


  implicit def avroValueTraverse[S]:Traverse[AvroValue[S, ?]] = new Traverse[AvroValue[S, ?]] {
    override def traverseImpl[G[_]:Applicative,A,B](fa:AvroValue[S, A])(f: A => G[B]): G[AvroValue[S,B]] = { 
      val applicativeG = Applicative[G]
      fa match {
        case AvroNullValue(s) => applicativeG.pure(AvroNullValue[S, B](s))
        case AvroBooleanValue(s, b) => applicativeG.pure(AvroBooleanValue[S, B](s, b))
        case AvroIntValue(s, i) =>  applicativeG.pure(AvroIntValue[S, B](s, i))
        case AvroLongValue(s, l) => applicativeG.pure(AvroLongValue[S, B](s, l))
        case AvroFloatValue(s, f) => applicativeG.pure(AvroFloatValue[S, B](s, f))
        case AvroDoubleValue(s, d) => applicativeG.pure(AvroDoubleValue[S, B](s, d))
        case AvroBytesValue(s, bytes) => applicativeG.pure(AvroBytesValue[S, B](s, bytes))
        case AvroStringValue(s, value) => applicativeG.pure(AvroStringValue[S, B](s, value))
        case AvroRecordValue(s, fields) => applicativeG.map(Traverse[ListMap[String, ?]].traverse(fields)(f))(flds => AvroRecordValue[S, B](s, flds))
        case AvroEnumValue(s, symbol) => applicativeG.pure(AvroEnumValue[S, B](s, symbol))
        case AvroArrayValue(s, items) =>  applicativeG.map(Traverse[List].traverse(items)(f))(itms => AvroArrayValue[S, B](s, itms)) 
        case AvroMapValue(s, values) =>   applicativeG.map(Traverse[Map[String, ?]].traverse(values)(f))(vals => AvroMapValue[S, B](s, vals))
        case AvroUnionValue(s, member) => applicativeG.map(f(member))(AvroUnionValue[S, B](s, _))
        case AvroFixedValue(s, bytes) => applicativeG.pure(AvroFixedValue[S, B](s, bytes))
      }
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
