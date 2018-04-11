package com
package scigility
package fp_avro


import matryoshka._
import matryoshka.implicits._
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.Schema.Field.Order
import org.apache.avro.Schema.Field
import Data._
import org.apache.avro.generic.GenericRecord
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap


/**
  *  This is a collection of Algebras to use with FP-Avro:
  *  e.g. to get from a schema to an internal representation
**/
object AvroAlgebra {
  val avroSchemaToInternalType:Coalgebra[AvroType, Schema] = (schema:Schema) => schema.getType match {
    case Type.NULL => AvroNull()
    case Type.BOOLEAN => AvroBoolean()
    case Type.INT => AvroInt()
    case Type.LONG => AvroLong()
    case Type.FLOAT => AvroFloat()
    case Type.DOUBLE => AvroDouble()
    case Type.BYTES => AvroBytes()
    case Type.STRING => AvroString()
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
      AvroRecord(nameSpace, name, doc, aliases, fields)
    }

    case Type.ENUM => AvroEnum(schema.getNamespace, schema.getName, Option(schema.getDoc), Option(schema.getAliases).map(_.asScala.toSet), schema.getEnumSymbols.asScala.toList)
    case Type.ARRAY => AvroArray(schema.getElementType)
    case Type.MAP => AvroMap(schema.getValueType)
    case Type.UNION => AvroUnion(schema.getTypes.asScala.toList)
    case Type.FIXED => AvroFixed(schema.getNamespace, schema.getName, Option(schema.getAliases).map(_.asScala.toSet), schema.getFixedSize)
 
  }



}
