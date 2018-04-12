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
import shapeless.Typeable
import syntax.traverse._
import shapeless.Typeable._
/**
  *  This is a collection of Algebras to use with FP-Avro:
  *  e.g. to get from a schema to an internal representation
**/
//BUG: will crash if used on in-code generated GenericRecord. I suspect that the change from String to avro.util.UTF8 happens during serialization. Will have to build in some sort of runtime type mappings
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
 //FIXME: Requires refactor pass
  def avroGenericReprToInternal[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]):CoalgebraM[Either[String, ?], AvroValue[F[AvroType], ?], (F[AvroType], Any)] = (tp:(F[AvroType], Any)) => { 
    //Check if a value is not null and can be casted to type T 
    //FIXME: provide instances and reenable?
   /* def castValue[T: Typeable](rawJavaValue:Any, schema:AvroType[F[AvroType]], nullHint:String, typeHint:String)(implicit tt:TypeTag[T]):Either[String, (F[AvroType], T)] = {
      val rawE = if (rawJavaValue != null) Right(rawJavaValue) else Left(s"$nullHint was null on selection")
      val casted = rawE.flatMap( 
        raw => { 
          Typeable[T].cast(raw) match {
            case Some(value) => Right(value)
            case None => Left(s"$typeHint was not of Type ${tt.tpe}")
          }
        }
      )
      casted.map(x => (birec.embed(schema), x))
    }*/

    def castValue[T](rawJavaValue:Any, schema:AvroType[F[AvroType]], nullHint:String, typeHint:String)(implicit tt:TypeTag[T]):Either[String, (F[AvroType], T)] = {
      val rawE = if (rawJavaValue != null) Right(rawJavaValue) else Left(s"$nullHint was null on selection")
      val casted = rawE.flatMap( 
        raw => { 
          try {
            Right(raw.asInstanceOf[T])
          } catch {
            case _ => Left(s"$typeHint was not of Type ${tt.tpe}")
          }
        }
      )
      casted.map(x => (birec.embed(schema), x))
    }

    

    val outerSchema = birec.project(tp._1) 
    outerSchema match {
      case nullSchema:AvroNullType[F[AvroType]] => Right(AvroNullValue(nullSchema))
      case booleanSchema:AvroBooleanType[F[AvroType]] => castValue[Boolean](tp._2, outerSchema, "booleanInstance", "booleanInstance").map(tpl => AvroBooleanValue(booleanSchema, tpl._2))
      case intSchema:AvroIntType[F[AvroType]] => castValue[Int](tp._2, outerSchema, "intInstance", "intInstance").map(tpl => AvroIntValue(intSchema, tpl._2))
      case longSchema:AvroLongType[F[AvroType]] => castValue[Long](tp._2, outerSchema, "longInstance", "longInstance").map(tpl => AvroLongValue(longSchema, tpl._2))
      case floatSchema:AvroFloatType[F[AvroType]] => castValue[Float](tp._2, outerSchema, "floatInstace", "floatInstance").map(tpl => AvroFloatValue(floatSchema, tpl._2))
      case doubleSchema:AvroDoubleType[F[AvroType]] => castValue[Double](tp._2, outerSchema, "doubleInstance", "doubleInstance").map(tpl => AvroDoubleValue(doubleSchema, tpl._2))
      case bytesSchema:AvroBytesType[F[AvroType]] => castValue[java.nio.ByteBuffer](tp._2, outerSchema, "bytesInstance", "bytesInstance").map(tpl => AvroBytesValue(bytesSchema, tpl._2.array.toVector))
      case stringSchema:AvroStringType[F[AvroType]] => castValue[org.apache.avro.util.Utf8](tp._2, outerSchema, "stringInstance", "stringInstance").map(tpl => AvroStringValue(stringSchema, tpl._2.toString))
      case rec:AvroRecordType[F[AvroType]] =>  {

        //refines an arbitrary AvroType and its value checking if the types match up and wrapping them in an either with the Any being casted to the expected input type of the outerSchema patternmatch 
        def refineInstance(value:Any, schema:AvroType[F[AvroType]], nullHint:String, typeHint:String ): Either[String, (F[AvroType], Any)] = schema match {
          case AvroNullType() => Right((birec.embed(schema), null))
          case AvroBooleanType() => castValue[Boolean](value, schema, nullHint, typeHint)
          case AvroIntType() => castValue[Int](value, schema, nullHint, typeHint)
          case AvroLongType() => castValue[Long](value, schema, nullHint, typeHint)
          case AvroFloatType() => castValue[Float](value, schema, nullHint, typeHint)
          case AvroDoubleType() => castValue[Double](value, schema, nullHint, typeHint)
          case AvroBytesType() => castValue[java.nio.ByteBuffer](value, schema, nullHint, typeHint)
          case AvroStringType() => castValue[org.apache.avro.util.Utf8](value, schema, nullHint, typeHint)
          case _: AvroRecordType[F[AvroType]] =>  castValue[GenericRecord](value, schema, nullHint, typeHint)
          case _: AvroEnumType[F[AvroType]] => castValue[GenericData.EnumSymbol](value, schema, nullHint, typeHint).map(tpl => tpl._1 -> tpl._2.toString)
          case _: AvroArrayType[F[AvroType]] => castValue[GenericData.Array[Any]](value, schema, nullHint, typeHint).map(tpl => tpl._1 -> tpl._2.iterator.asScala.toList)
          case _: AvroMapType[F[AvroType]] => castValue[java.util.HashMap[String, AnyRef]](value, schema, nullHint, typeHint).map(tpl => tpl._1 -> tpl._2.asScala.toMap)
          case _: AvroUnionType[F[AvroType]] => ??? //FIXME: how do Unions work? are they unified to Object and are at runtime one of the members specified on the schema?
          case _: AvroFixedType[F[AvroType]] => castValue[GenericData.Fixed](value, schema, nullHint, typeHint).map(tpl => tpl._1 -> tpl._2.bytes().toVector)
        }

        val shouldBeRec = if (tp._2.isInstanceOf[GenericRecord]) Right(tp._2.asInstanceOf[GenericRecord]) else Left(s"Passed object ${tp._2} was not castable to GenericRecord. Schema: $rec")
        shouldBeRec.flatMap(
          gRec => {
            val lmTraverse = Traverse[ListMap[String, ?]]
            //these are allowed to be hetrogenous
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
      case enumSchema:AvroEnumType[F[AvroType]] => castValue[String](tp._2, outerSchema, "enumInstance", "enumInstance").map(tpl => AvroEnumValue(enumSchema, tpl._2))
      case arraySchema:AvroArrayType[F[AvroType]] => {
        val nullHint = "arrayElement"
        val typeHint = "arrayElement"
        // this was hetrogenous elements should be detected in the List[Any]
        val refineInstance: Any => Either[String, (F[AvroType], Any)] = arraySchema.items match {
          case AvroNullType() => _ =>  Right((birec.embed(outerSchema), null))
          case AvroBooleanType() => value => castValue[Boolean](value, outerSchema, nullHint, typeHint)
          case AvroIntType() => value => castValue[Int](value, outerSchema, nullHint, typeHint)
          case AvroLongType() => value => castValue[Long](value, outerSchema, nullHint, typeHint)
          case AvroFloatType() => value => castValue[Float](value, outerSchema, nullHint, typeHint)
          case AvroDoubleType() => value => castValue[Double](value, outerSchema, nullHint, typeHint)
          case AvroBytesType() => value => castValue[java.nio.ByteBuffer](value, outerSchema, nullHint, typeHint)
          case AvroStringType() => value => castValue[org.apache.avro.util.Utf8](value, outerSchema, nullHint, typeHint)
          case _: AvroRecordType[F[AvroType]] => value =>  castValue[GenericRecord](value, outerSchema, nullHint, typeHint)
          case _: AvroEnumType[F[AvroType]] => value => castValue[GenericData.EnumSymbol](value, outerSchema, nullHint, typeHint).map(tpl =>  tpl._1 -> tpl._2.toString)
          case _: AvroArrayType[F[AvroType]] => value => castValue[GenericData.Array[Any]](value, outerSchema, nullHint, typeHint).map(tpl =>  tpl._1 -> tpl._2.iterator.asScala.toList)
          case _: AvroMapType[F[AvroType]] => value => castValue[java.util.HashMap[String, AnyRef]](value, outerSchema, nullHint, typeHint).map(tpl =>  tpl._1 -> tpl._2.asScala.toMap)
          case _: AvroUnionType[F[AvroType]] => value => ??? //FIXME: how do Unions work? are they unified to Object and are at runtime one of the members specified on the outerSchema?
          case _: AvroFixedType[F[AvroType]] => value => castValue[GenericData.Fixed](value, outerSchema, nullHint, typeHint).map(tpl =>  tpl._1 -> tpl._2.bytes().toVector)
        }

        val elemsE = if (tp._2.isInstanceOf[List[Any]]) Right(tp._2.asInstanceOf[List[Any]]) else Left(s"Passed object ${tp._2} was not castable to List[Any]. Schema: $arraySchema")
        elemsE.flatMap(elem => Traverse[List].traverse(elem)(refineInstance)).map( elems => AvroArrayValue(arraySchema, elems) )
      }
      case mapSchema: AvroMapType[F[AvroType]] => {
        val nullHint = "arrayElement"
        val typeHint = "arrayElement"
        // this was hetrogenous elements should be detected in the List[Any]
        val refineInstance: Any => Either[String, (F[AvroType], Any)] = mapSchema.values match {
          case AvroNullType() => _ =>  Right((birec.embed(outerSchema), null))
          case AvroBooleanType() => value => castValue[Boolean](value, outerSchema, nullHint, typeHint)
          case AvroIntType() => value => castValue[Int](value, outerSchema, nullHint, typeHint)
          case AvroLongType() => value => castValue[Long](value, outerSchema, nullHint, typeHint)
          case AvroFloatType() => value => castValue[Float](value, outerSchema, nullHint, typeHint)
          case AvroDoubleType() => value => castValue[Double](value, outerSchema, nullHint, typeHint)
          case AvroBytesType() => value => castValue[java.nio.ByteBuffer](value, outerSchema, nullHint, typeHint)
          case AvroStringType() => value => castValue[org.apache.avro.util.Utf8](value, outerSchema, nullHint, typeHint)
          case _: AvroRecordType[_] => value =>  castValue[GenericRecord](value, outerSchema, nullHint, typeHint)
          case _: AvroEnumType[F[AvroType]] => value => castValue[GenericData.EnumSymbol](value, outerSchema, nullHint, typeHint).map(tpl =>  tpl._1 -> tpl._2.toString)
          case _: AvroArrayType[F[AvroType]] => value => castValue[GenericData.Array[Any]](value, outerSchema, nullHint, typeHint).map(tpl =>  tpl._1 -> tpl._2.iterator.asScala.toList)
          case _: AvroMapType[F[AvroType]] => value => castValue[java.util.HashMap[String, AnyRef]](value, outerSchema, nullHint, typeHint).map(tpl =>  tpl._1 -> tpl._2.asScala.toMap)
          case _: AvroUnionType[F[AvroType]] => value => ??? //FIXME: how do Unions work? are they unified to Object and are at runtime one of the members specified on the outerSchema?
          case _: AvroFixedType[F[AvroType]] => value => castValue[GenericData.Fixed](value, outerSchema, nullHint, typeHint)
        }

        val elemsE = if (tp._2.isInstanceOf[Map[String, Any]]) Right(tp._2.asInstanceOf[Map[String, Any]]) else Left(s"Passed object ${tp._2} was not castable to Map[String, Any]. Schema: $mapSchema")
        elemsE.flatMap(elem => Traverse[Map[String,?]].traverse(elem)(refineInstance)).map( elems => AvroMapValue(mapSchema, elems) )
      }
      case unionSchema: AvroUnionType[F[AvroType]] => ??? //FIXME: Still the union issue
      case fixedSchema: AvroFixedType[F[AvroType]] => castValue[GenericData.Fixed](tp._2, outerSchema, "fixedInstance", "fixedInstance").map(tpl => AvroFixedValue(fixedSchema, tpl._2.bytes.toVector))
    }
  }


}
