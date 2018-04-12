package com
package scigility
package fp_avro


import matryoshka.{ Recursive, _ }
import matryoshka.data.Fix
import matryoshka.implicits._
import matryoshka.patterns.EnvT
import org.apache.avro.{ Schema }
import org.apache.avro.Schema.Type
import org.apache.avro.Schema.Field.Order
import org.apache.avro.Schema.Field
import org.apache.avro.generic.GenericData
import Data._
import org.apache.avro.generic._
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import implicits._
import scala.reflect.runtime.universe._
import scalaz._
import Scalaz._
import syntax.traverse._
/**
  *  This is a collection of Algebras to use with FP-Avro:
  *  e.g. to get from a schema to an internal representation
**/
object AvroAlgebra {
  val avroSchemaToInternalType:Coalgebra[AvroType, Schema] = (schema:Schema) => schema.getType match {
    case Type.NULL => AvroNullType()
    case Type.BOOLEAN => AvroBooleanType()
    case Type.INT => AvroIntType()
    case Type.LONG => AvroLongType()
    case Type.FLOAT => AvroFloatType()
    case Type.DOUBLE => AvroDoubleType()
    case Type.BYTES => AvroBytesType()
    case Type.STRING => AvroStringType()
    case Type.RECORD => {
      val nameSpace = schema.getNamespace
      val name = schema.getName
      val doc = Option(schema.getDoc)
      val aliases = Option(schema.getAliases).map(_.asScala.toSet)
      val fields = schema.getFields.asScala.toSet.map(
        (fld:Field) => {
          val fldName =  fld.name
          val fldDoc = Option(fld.doc)
          val fldDefultExpr = None //FIXME: Evaulate Json node to value. Gotta figure out how to type the default value
          val fldAlias = Option(fld.aliases()).map(_.asScala.toSet)
          val fldSortOrder = Option(fld.order()).map {
            case Order.ASCENDING => ARSOAscending
            case Order.DESCENDING => ARSODescending
            case Order.IGNORE => ARSOIgnore
          }
          AvroRecordFieldMetaData(fldName, fldDoc, fldDefultExpr, fldSortOrder, fldAlias) -> fld.schema
        }
      ).foldRight(ListMap.empty[AvroRecordFieldMetaData, Schema]) (
        (elem, map) => map + elem
      )
      AvroRecordType(nameSpace, name, doc, aliases, fields)
    }

    case Type.ENUM => AvroEnumType(schema.getNamespace, schema.getName, Option(schema.getDoc), Option(schema.getAliases).map(_.asScala.toSet), schema.getEnumSymbols.asScala.toList)
    case Type.ARRAY => AvroArrayType(schema.getElementType)
    case Type.MAP => AvroMapType(schema.getValueType)
    case Type.UNION => AvroUnionType(schema.getTypes.asScala.toList)
    case Type.FIXED => AvroFixedType(schema.getNamespace, schema.getName, Option(schema.getAliases).map(_.asScala.toSet), schema.getFixedSize) 
  }

  //God I feel so dirty doing this. This needs somebody more advanced in typemagic
//  private[this] def cast[A,B](a:A)(implicit tta:TypeTag[A], ttb:TypeTag[B]):Either[String, B] = if (a.isInstanceOf[B]) Right(a.asInstanceOf[B]) else Left(s"cast failed: a  = ${tta.tpe}, b = ${ttb.tpe}")
 
  def avroGenericReprToInternal[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]):CoalgebraM[Either[String, ?], AvroValue[F[AvroType], ?], (F[AvroType], Any)] = (tp:(F[AvroType], Any)) => { 

    def castValue[T](rawJavaValue:AnyRef, schema:AvroType[F[AvroType]], nullHint:String, typeHint:String)(implicit tt:TypeTag[T]):Either[String, (F[AvroType], T)] = {
      val rawE = if (rawJavaValue != null) Right(rawJavaValue) else Left(s"$nullHint was null on GenericRecord")
      val casted = rawE.flatMap( raw => if (raw.isInstanceOf[T]) Right(raw.asInstanceOf[T]) else Left(s"$typeHint was not of Type ${tt.tpe}"))
      casted.map(x => (birec.embed(schema), x))
    }

    def refineInstance(value:AnyRef, schema:AvroType[F[AvroType]], nullHint:String, typeHint:String ): Either[String, (F[AvroType], Any)] = schema match {
      case AvroNullType() => Right((birec.embed(schema), null))
      case AvroBooleanType() => castValue[Boolean](value, schema, nullHint, typeHint)
      case AvroIntType() => castValue[Int](value, schema, nullHint, typeHint)
      case AvroLongType() => castValue[Long](value, schema, nullHint, typeHint)
      case AvroFloatType() => castValue[Float](value, schema, nullHint, typeHint)
      case AvroDoubleType() => castValue[Double](value, schema, nullHint, typeHint)
      case AvroBytesType() => castValue[java.nio.ByteBuffer](value, schema, nullHint, typeHint).map(tpl => tpl._1 -> tpl._2.array.toVector)
      case AvroStringType() => castValue[org.apache.avro.util.Utf8](value, schema, nullHint, typeHint).map(tpl => tpl._1 -> tpl._2.toString)
      case _: AvroRecordType[F[AvroType]] =>  castValue[GenericRecord](value, schema, nullHint, typeHint)
      case _: AvroEnumType[F[AvroType]] => castValue[GenericData.EnumSymbol](value, schema, nullHint, typeHint).map(tpl => tpl._1 -> tpl._2.toString)
      case _: AvroArrayType[F[AvroType]] => castValue[GenericData.Array[AnyRef]](value, schema, nullHint, typeHint).map(tpl => tpl._1 -> tpl._2.iterator.asScala.toList)
      case _: AvroMapType[F[AvroType]] => castValue[java.util.HashMap[String, AnyRef]](value, schema, nullHint, typeHint).map(tpl => tpl._1 -> tpl._2.asScala.toMap)
      case _: AvroUnionType[F[AvroType]] => ???
      case _: AvroFixedType[F[AvroType]] => castValue[GenericData.Fixed](value, schema, nullHint, typeHint).map(tpl => tpl._1 -> tpl._2.bytes().toVector)

    }

     
    birec.project(tp._1) match {
      case rec:AvroRecordType[F[AvroType]] =>  {
        val shouldBeRec = if (tp._2.isInstanceOf[GenericRecord]) Right(tp._2.asInstanceOf[GenericRecord]) else Left(s"Passed object ${tp._2} was not castable to GenericRecord. Schema: $rec")
        shouldBeRec.flatMap(
          gRec => {
            val lmTraverse = Traverse[ListMap[String, ?]]

            val fields = lmTraverse.traverse(rec.fields.map( kv => (kv._1.name, (kv._1, kv._2)))) (
              kv => {
                val fieldName = kv._1.name
                val schema:AvroType[F[AvroType]] = birec.project(kv._2)
                val value = gRec.get(fieldName)
                refineInstance(value, schema, fieldName, fieldName)
              }
            )
            fields.map( flds => AvroRecordValue(rec,flds))
          }
        )
      }
    }
  }


}
