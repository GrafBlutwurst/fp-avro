package com
package scigility
package fp_avro

/*import eu.timepit.refined.api.Refined
import eu.timepit.refined.api.Validate*/
import scala.collection.immutable.ListMap

object Data{

  //Required Refinements
  final case class AvroValidUnion()

  /**
    * This makes sure that a List of AvroTypes actually forms a valid Union as defined in
    *  https://avro.apache.org/docs/1.8.1/spec.html#Unions
  **/
  /*implicit val validateUnionMembers: Validate.Plain[List[AvroType], AvroValidUnion] = 
    Validate.fromPredicate(
      lst =>{ 
        lst.filter{ case AvroUnion(_) => true }.length == 0 && // avro unions may not contain any other unions directly
        lst.map{ 
          case AvroNull => 1
          case AvroBoolean => 2
          case AvroInt => 3
          case AvroLong => 4
          case AvroFloat => 5
          case AvroDouble => 6
          case AvroBytes => 7
          case AvroString => 8
          case _: AvroMap => 9
          case _: AvroArray => 10
          case _ => 0
        }
          .filter( _> 0)
          .groupBy(identity)
          .forall(_._2.length == 1) && // make sure there are no non-named avro typed double in the union
        lst.map{ 
          case rec: AvroRecord => (rec.namespace, rec.name)
          case enum: AvroEnum => (enum.namespace, enum.name)
          case fixed:AvroFixed => (fixed.namespace, fixed.name)
          case _ => ("", "")
        }
          .filter( tp => tp._1 != "" && tp._2 != "")
          .groupBy(identity)
          .forall(_._2 == 1) // make sure that named members (enums, fixed and records) do not appear more than once
      },
      lst => s"$lst is a valid list of union members", 
      AvroValidUnion()
    )*/

  //Avro Types required to represent Schemata
  sealed trait AvroType[A]

  sealed trait AvroPrimitiveType[A] extends AvroType[A]
  final case class AvroNull[A]() extends AvroPrimitiveType[A]
  final case class AvroBoolean[A]() extends AvroPrimitiveType[A]
  final case class AvroInt[A]() extends AvroPrimitiveType[A]
  final case class AvroLong[A]() extends AvroPrimitiveType[A]
  final case class AvroFloat[A]() extends AvroPrimitiveType[A]
  final case class AvroDouble[A]() extends AvroPrimitiveType[A]
  final case class AvroBytes[A]() extends AvroPrimitiveType[A]
  final case class AvroString[A]() extends AvroPrimitiveType[A]


  sealed trait AvroComplexType[A] extends AvroType[A]
  final case class AvroRecord[A](namespace:String, name:String, doc:Option[String], aliases:Option[Set[String]], fields:ListMap[AvroRecordFieldMetaData, A]) extends AvroComplexType[A]
  final case class AvroEnum[A](namespace:String, name:String, doc:Option[String], aliases:Option[Set[String]], symbols:List[String]) extends AvroComplexType[A]
  final case class AvroArray[A](items:A) extends AvroComplexType[A]
  final case class AvroMap[A](values:A) extends AvroComplexType[A]
  final case class AvroUnion[A](members:List[A] /*Refined AvroValidUnion*/) extends AvroComplexType[A]
  final case class AvroFixed[A](namespace: String, name:String, aliases:Option[Set[String]], length:Int) extends AvroComplexType[A]


  final case class AvroRecordFieldMetaData(name:String, doc:Option[String], default:Option[String], order:Option[AvroRecordSortOrder], aliases:Option[Set[String]]) //FIXME: default should somehow have something to do with the Avro type? does Default work for complex types? e.g. a field that is itself a records? if so how is it represented? JSON encoding? In schema it's a JSON Node. Evaluating that might require the recursive Datatype for instances we still have to do

  //helpers
  sealed trait AvroRecordSortOrder
  final case object ARSOAscending extends AvroRecordSortOrder
  final case object ARSODescending extends AvroRecordSortOrder
  final case object ARSOIgnore extends AvroRecordSortOrder



  sealed trait AvroGenericRecord[A]

  sealed trait AvroGenericRecordPrimitive[A] extends AvroGenericRecord[A]
  final case class AvroNullValue[A]() extends AvroGenericRecordPrimitive[A]
  final case class AvroBooleanValue[A](value:Boolean) extends AvroGenericRecordPrimitive[A]
  final case class AvroIntValue[A](value:Int) extends AvroGenericRecordPrimitive[A]
  final case class AvroLongValue[A](value:Long) extends AvroGenericRecordPrimitive[A]
  final case class AvroFloatValue[A](value:Float) extends AvroGenericRecordPrimitive[A]
  final case class AvroDoubleValue[A](value:Double) extends AvroGenericRecordPrimitive[A]
  final case class AvroBytesValue[A](value: Vector[Byte]) extends AvroGenericRecordPrimitive[A]
  final case class AvroStringValue[A](value: String) extends AvroGenericRecordPrimitive[A]

  sealed trait AvroGenericRecordComplex[A] extends AvroGenericRecord[A]
  final case class AvroRecordValue[A](fields:ListMap[String, AvroGenericRecord[_]]) extends AvroGenericRecordComplex[A] //FIXME: this is not right. Fields can be of differing types. 
  final case class AvroEnumValue[A](symbol:String) extends AvroGenericRecordComplex[A]
  final case class AvroArrayValue[A](items:List[A]) extends AvroGenericRecordComplex[A]
  final case class AvroMapValue[A](values:Map[String, A]) extends AvroGenericRecordComplex[A]
  final case class AvroUnionValue[A](member:A) extends AvroGenericRecordComplex[A]
  final case class AvroFixedValue[A](bytes:Vector[Byte]) extends AvroGenericRecordComplex[A] //FIXME: looks a lot like regular bytes. doublecheck in specification





  sealed trait AvroSchema
  final case class AvroSchemaReference(name:String) extends AvroSchema
  final case class AvroSchemaUnion() extends AvroSchema
  final case class AvroSchemaType[A](aType:A) extends AvroSchema

}
