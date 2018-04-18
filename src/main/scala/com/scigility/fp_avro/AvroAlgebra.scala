package com
package scigility
package fp_avro


import matryoshka._
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
import shapeless.Typeable
import shapeless.Typeable._
/**
  *  This is a collection of Algebras to use with FP-Avro:
  *  e.g. to get from a schema to an internal representation
**/
//BUG: will crash if used on in-code generated GenericRecord. I suspect that the change from String to avro.util.UTF8 happens during serialization. Will have to build in some sort of runtime type mappings
object AvroAlgebra {

  /**
    * This Coalgebra lets you unfold a org.apache.avro.Schema instance into the recursive AvroType Datastructure. This is needed to later unfold value types into the AvroValue Structure
    * e.g. schema.ana[Fix[AvroType]](AvroAlgebra.avroSchemaToInternalType)
    * 
  **/
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
      val fields = schema.getFields.asScala.toList.map(
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
      ).foldLeft(ListMap.empty[AvroRecordFieldMetaData, Schema]) (
        (map, elem) => map + elem
      )
      AvroRecordType(nameSpace, name, doc, aliases, fields)
    }

    case Type.ENUM => AvroEnumType(schema.getNamespace, schema.getName, Option(schema.getDoc), Option(schema.getAliases).map(_.asScala.toSet), schema.getEnumSymbols.asScala.toList)
    case Type.ARRAY => AvroArrayType(schema.getElementType)
    case Type.MAP => AvroMapType(schema.getValueType)
    case Type.UNION => AvroUnionType(schema.getTypes.asScala.toList)
    case Type.FIXED => AvroFixedType(schema.getNamespace, schema.getName, Option(schema.getDoc), Option(schema.getAliases).map(_.asScala.toSet), schema.getFixedSize) 
  }

 //FIXME: Requires refactor pass (error reporting, think if we can streamline the reverse union matching )
  /**
    * This Coalgebra lets you unfold a Pair of (AvroType, Any) into Either[Error, F[AvroValue]] where F is your chosen Fixpoint. The instance of Any has to correlate to what the AvroType represents. e.g. if the Schema represents a Record, any has to be a GenericData.Record.
    * usage: (schemaInternal, deserializedGenRec).anaM[Fix[AvroValue[Fix[AvroType], ?]]](AvroAlgebra.avroGenericReprToInternal[Fix]) though I'll prorbably make some convinience functions down the line to make this a bit easier
  **/
  def avroGenericReprToInternal[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]):CoalgebraM[Either[String, ?], AvroValue[F[AvroType], ?], (F[AvroType], Any)] = (tp:(F[AvroType], Any)) => { 

    def castValue[T: Typeable](rawJavaValue:Any, schema:AvroType[F[AvroType]])(implicit tt:TypeTag[T]):Either[String, (F[AvroType], T)] = {
      val rawE = if (rawJavaValue != null) Right(rawJavaValue) else Left(s"item at $schema was null on selection")
      rawE.flatMap( 
        raw => Typeable[T].cast(raw) match {
          case Some(t) => Right((birec.embed(schema), t))
          case None => Left(s"Could not cast value $raw to ${tt.tpe}")
        }
      )
    }
    val outerSchema = birec.project(tp._1) 

    def refineInstance(componentSchema:AvroType[F[AvroType]]): Any => Either[String, (F[AvroType], Any)] = componentSchema match {
          case AvroNullType() => _ =>  Right((birec.embed(componentSchema), null))
          case AvroBooleanType() => value => castValue[Boolean](value, componentSchema)
          case AvroIntType() => value => castValue[Int](value, componentSchema)
          case AvroLongType() => value => castValue[Long](value, componentSchema)
          case AvroFloatType() => value => castValue[Float](value, componentSchema)
          case AvroDoubleType() => value => castValue[Double](value, componentSchema)
          case AvroBytesType() => value => castValue[java.nio.ByteBuffer](value, componentSchema)
          case AvroStringType() => value => castValue[org.apache.avro.util.Utf8](value, componentSchema)
          case _: AvroRecordType[_] => value =>  castValue[GenericData.Record](value, componentSchema)
          case _: AvroEnumType[_] => value => castValue[GenericData.EnumSymbol](value, componentSchema)
          case _: AvroArrayType[_] => value => castValue[GenericData.Array[Any]](value, componentSchema)
          case _: AvroMapType[_] => value => castValue[java.util.HashMap[String, Any]](value, componentSchema)
          case _: AvroUnionType[_] => value => Right((birec.embed(componentSchema), value)) //In case of the union it needs to be handled downstream. a union is represented as just a java.lang.object and can actually be any of it's members
          case _: AvroFixedType[_] => value => castValue[GenericData.Fixed](value, componentSchema)
        }


    outerSchema match {
      case nullSchema:AvroNullType[F[AvroType]] => Right(AvroNullValue(nullSchema))
      case booleanSchema:AvroBooleanType[F[AvroType]] => castValue[Boolean](tp._2, outerSchema).map(tpl => AvroBooleanValue(booleanSchema, tpl._2))
      case intSchema:AvroIntType[F[AvroType]] => castValue[Int](tp._2, outerSchema).map(tpl => AvroIntValue(intSchema, tpl._2))
      case longSchema:AvroLongType[F[AvroType]] => castValue[Long](tp._2, outerSchema).map(tpl => AvroLongValue(longSchema, tpl._2))
      case floatSchema:AvroFloatType[F[AvroType]] => castValue[Float](tp._2, outerSchema).map(tpl => AvroFloatValue(floatSchema, tpl._2))
      case doubleSchema:AvroDoubleType[F[AvroType]] => castValue[Double](tp._2, outerSchema).map(tpl => AvroDoubleValue(doubleSchema, tpl._2))
      case bytesSchema:AvroBytesType[F[AvroType]] => castValue[java.nio.ByteBuffer](tp._2, outerSchema).map(tpl => AvroBytesValue(bytesSchema, tpl._2.array.toVector))
      case stringSchema:AvroStringType[F[AvroType]] => castValue[org.apache.avro.util.Utf8](tp._2, outerSchema).map(tpl => AvroStringValue(stringSchema, tpl._2.toString))
      case rec:AvroRecordType[F[AvroType]] =>  {
        val shouldBeRec = castValue[GenericData.Record](tp._2, rec).map(_._2)
        shouldBeRec.flatMap(
          gRec => {
            val lmTraverse = Traverse[ListMap[String, ?]]
            //these are allowed to be hetrogenous
            val fields = lmTraverse.traverse(rec.fields.map( kv => (kv._1.name, (kv._1, kv._2)))) (
              kv => {
                val fieldName = kv._1.name
                val schema:AvroType[F[AvroType]] = birec.project(kv._2)
                val value = gRec.get(fieldName)
                refineInstance(schema)(value)
              }
            )
            fields.map( flds => AvroRecordValue(rec,flds))
          }
        )
      }
      case enumSchema:AvroEnumType[F[AvroType]] => castValue[GenericData.EnumSymbol](tp._2, outerSchema).map(tpl => AvroEnumValue(enumSchema, tpl._2.toString))
      case arraySchema:AvroArrayType[F[AvroType]] => {
        val refinement = refineInstance(birec.project(arraySchema.items))
        val elemsE = castValue[GenericData.Array[Any]](tp._2, arraySchema).map(_._2.iterator.asScala.toList)
        elemsE.flatMap(elem => Traverse[List].traverse(elem)(refinement)).map( elems => AvroArrayValue(arraySchema, elems) )
      }
      case mapSchema: AvroMapType[F[AvroType]] => {
        val refinement = refineInstance(birec.project(mapSchema.values))
        val elemsE = castValue[java.util.HashMap[String, Any]](tp._2, mapSchema).map(_._2.asScala.toMap)
        elemsE.flatMap(elem => Traverse[Map[String,?]].traverse(elem)(refinement)).map( elems => AvroMapValue(mapSchema, elems) )
      }
      case unionSchema: AvroUnionType[F[AvroType]] => {
        //In case of a union we need to reverse match the whole thing. take the value and match it's type against the described members in the unionSchema. if there's a fit apply it if not throw an error
        //For types that can occur multiple times in a union (Fixed, Enum, Record) we'll match on namespace and name
        tp._2 match { //FIXME: Think how we can make this a bit smaller
          case null => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case AvroNullType() => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a NullType")
            )(
              memberSchema => refineInstance(memberSchema)(null).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:Boolean => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case AvroBooleanType() => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a BooleanType")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:Int => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case AvroIntType() => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a IntType")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:Long => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case AvroLongType() => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a LongType")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:Float => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case AvroFloatType() => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a FloatType")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:Double => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case AvroDoubleType() => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a DoubleType")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:java.nio.ByteBuffer => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case AvroBytesType() => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a BytesType")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:org.apache.avro.util.Utf8 => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case AvroStringType() => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a StringType")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:GenericData.Record => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case recordType:AvroRecordType[_] => unionVal.getSchema.getNamespace == recordType.namespace && unionVal.getSchema.getName == recordType.name
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a a Record type allowing this generic record to be properly decoded")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:GenericData.EnumSymbol => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case enumType:AvroEnumType[_] => unionVal.getSchema.getNamespace == enumType.namespace && unionVal.getSchema.getName == enumType.name
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain an EnumType of the proper namespace and name")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:GenericData.Array[_] => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case _:AvroArrayType[_] => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain an ArrayType")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:java.util.HashMap[_, _] => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case _:AvroMapType[_] => true
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain a Map Type")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

          case unionVal:GenericData.Fixed => {
            val memberSchemaO = unionSchema.members.map(x => birec.project(x)).find(
              t => t match {
                case fixedType:AvroFixedType[_] => unionVal.getSchema.getNamespace == fixedType.namespace && unionVal.getSchema.getName == fixedType.name
                case _ => false
              }
            )

            memberSchemaO.fold[Either[String, AvroValue[F[AvroType], (F[AvroType], Any)]]](
              Left(s"Unresolved Union: ${unionSchema.members} did not contain an EnumType of the proper namespace and name")
            )(
              memberSchema => refineInstance(memberSchema)(unionVal).map( tpl => AvroUnionValue(unionSchema, tpl) )
            )
            
          }

        }
      }
      case fixedSchema: AvroFixedType[F[AvroType]] => castValue[GenericData.Fixed](tp._2, outerSchema).map(tpl => AvroFixedValue(fixedSchema, tpl._2.bytes.toVector))
    }
  }


  //FIXME: what about aliases?
  /**
    * This Algebra allows to fold a AvroType back down to a schema. not that a hylomorphism with avroSchemaToInternalType should yield the same schema again
  **/
  val avroTypeToSchema:Algebra[AvroType, Schema] = (avroType:AvroType[Schema]) => avroType match {
    case AvroNullType() => Schema.create(Schema.Type.NULL)
    case AvroBooleanType() => Schema.create(Schema.Type.BOOLEAN)
    case AvroIntType() => Schema.create(Schema.Type.INT)
    case AvroLongType() => Schema.create(Schema.Type.LONG)
    case AvroFloatType() => Schema.create(Schema.Type.FLOAT)
    case AvroDoubleType() => Schema.create(Schema.Type.DOUBLE)
    case AvroBytesType() => Schema.create(Schema.Type.BYTES)
    case AvroStringType() => Schema.create(Schema.Type.STRING)
    case rec:AvroRecordType[Schema] => {
      val flds = rec.fields.foldRight(List.empty[Schema.Field]) (
        (elemKv, lst) => {
          val elemMeta = elemKv._1
          val elemSchema = elemKv._2

          val sortOrder = elemMeta.order.map {
            case ARSOIgnore => Schema.Field.Order.IGNORE
            case ARSOAscending => Schema.Field.Order.ASCENDING
            case ARSODescending => Schema.Field.Order.DESCENDING
          }

          new Schema.Field(elemMeta.name, elemSchema, elemMeta.doc.getOrElse(null), elemMeta.default.getOrElse(null), sortOrder.getOrElse(null)) :: lst
        }
      )

      Schema.createRecord(rec.name, rec.doc.getOrElse(null), rec.namespace, false, flds.asJava)
    }
    case enum:AvroEnumType[_] => Schema.createEnum(enum.namespace, enum.name, enum.doc.getOrElse(null), enum.symbols.asJava)
    case arr:AvroArrayType[Schema] => Schema.createArray(arr.items)
    case map:AvroMapType[Schema] => Schema.createMap(map.values)
    case unionT:AvroUnionType[Schema] => Schema.createUnion(unionT.members.asJava)
    case fixed:AvroFixedType[_] => Schema.createFixed(fixed.name, fixed.doc.getOrElse(null), fixed.namespace, fixed.length)
  }


  /**
    * This is the algebra to fold the AvroValue back down to the generic representation which should be writable by Avoros serializers. note that Any is going to be dependent what kind avroValue you pass in.
  **/
  def avroValueToGenericRepr[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]):Algebra[AvroValue[F[AvroType], ?], Any] = (avroValue:AvroValue[F[AvroType],Any]) => avroValue match {
    case AvroNullValue(_) => null
    case AvroBooleanValue(_, b) => b
    case AvroIntValue(_, i) => i
    case AvroLongValue(_, l) => l
    case AvroFloatValue(_ , f) => f
    case AvroDoubleValue(_, d) => d
    case AvroBytesValue(_ , bs) => bs.toArray
    case AvroStringValue(_, s) => new org.apache.avro.util.Utf8(s)
    case AvroRecordValue(schema, flds) => flds.foldLeft(new GenericData.Record(birec.cata(birec.embed(schema))(avroTypeToSchema)))( (rec, fld) => {rec.put(fld._1, fld._2); rec})
    case AvroEnumValue(schema, symbol) => new GenericData.EnumSymbol(birec.cata(birec.embed(schema))(avroTypeToSchema), symbol)
    case AvroArrayValue(schema, items) => new GenericData.Array(birec.cata(birec.embed(schema))(avroTypeToSchema), items.asJava)
    case AvroMapValue(_, values) =>  values.asJava
    case AvroUnionValue(_, member) => member
    case AvroFixedValue(schema, bytes) => new GenericData.Fixed(birec.cata(birec.embed(schema))(avroTypeToSchema), bytes.toArray)
  }
  


}
