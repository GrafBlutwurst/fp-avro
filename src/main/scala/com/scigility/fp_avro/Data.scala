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
  //FIXME: Move this to a DSL when we get to the schema builder
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
  //TODO: extend with logical types and arbitraty properties
  sealed trait AvroType[A]

  sealed trait AvroPrimitiveType[A] extends AvroType[A]
  final case class AvroNullType[A]() extends AvroPrimitiveType[A]
  final case class AvroBooleanType[A]() extends AvroPrimitiveType[A]
  final case class AvroIntType[A]() extends AvroPrimitiveType[A]
  final case class AvroLongType[A]() extends AvroPrimitiveType[A]
  final case class AvroFloatType[A]() extends AvroPrimitiveType[A]
  final case class AvroDoubleType[A]() extends AvroPrimitiveType[A]
  final case class AvroBytesType[A]() extends AvroPrimitiveType[A]
  final case class AvroStringType[A]() extends AvroPrimitiveType[A]


  sealed trait AvroComplexType[A] extends AvroType[A]
  final case class AvroRecordType[A](namespace:String, name:String, doc:Option[String], aliases:Option[Set[String]], fields:ListMap[AvroRecordFieldMetaData, A]) extends AvroComplexType[A]
  final case class AvroEnumType[A](namespace:String, name:String, doc:Option[String], aliases:Option[Set[String]], symbols:List[String]) extends AvroComplexType[A]
  final case class AvroArrayType[A](items:A) extends AvroComplexType[A]
  final case class AvroMapType[A](values:A) extends AvroComplexType[A]
  final case class AvroUnionType[A](members:List[A] /*Refined AvroValidUnion*/) extends AvroComplexType[A]
  final case class AvroFixedType[A](namespace: String, name:String, doc:Option[String], aliases:Option[Set[String]], length:Int) extends AvroComplexType[A]


  final case class AvroRecordFieldMetaData(name:String, doc:Option[String], default:Option[String], order:Option[AvroRecordSortOrder], aliases:Option[Set[String]]) //FIXME: default should somehow have something to do with the Avro type? does Default work for complex types? e.g. a field that is itself a records? if so how is it represented? JSON encoding? In schema it's a JSON Node. Evaluating that might require the recursive Datatype for instances we still have to do

  //helpers
  sealed trait AvroRecordSortOrder
  final case object ARSOAscending extends AvroRecordSortOrder
  final case object ARSODescending extends AvroRecordSortOrder
  final case object ARSOIgnore extends AvroRecordSortOrder



  sealed trait AvroValue[S, A]

  sealed trait AvroPrimitiveValue[S, A] extends AvroValue[S, A]
  final case class AvroNullValue[S, A](schema:AvroNullType[S]) extends AvroPrimitiveValue[S, A]
  final case class AvroBooleanValue[S, A](schema:AvroBooleanType[S], value:Boolean) extends AvroPrimitiveValue[S, A]
  final case class AvroIntValue[S, A](schema:AvroIntType[S], value:Int) extends AvroPrimitiveValue[S, A]
  final case class AvroLongValue[S, A](schema:AvroLongType[S], value:Long) extends AvroPrimitiveValue[S, A]
  final case class AvroFloatValue[S, A](schema:AvroFloatType[S], value:Float) extends AvroPrimitiveValue[S, A]
  final case class AvroDoubleValue[S, A](schema:AvroDoubleType[S], value:Double) extends AvroPrimitiveValue[S, A]
  final case class AvroBytesValue[S, A](schema:AvroBytesType[S], value: Vector[Byte]) extends AvroPrimitiveValue[S, A]
  final case class AvroStringValue[S, A](schema:AvroStringType[S], value: String) extends AvroPrimitiveValue[S, A]

  sealed trait AvroComplexValue[S, A] extends AvroValue[S, A]
  final case class AvroRecordValue[S, A](schema:AvroRecordType[S], fields:ListMap[String, A]) extends AvroComplexValue[S, A] 
  final case class AvroEnumValue[S, A](schema:AvroEnumType[S], symbol:String) extends AvroComplexValue[S, A]
  final case class AvroArrayValue[S, A](schema:AvroArrayType[S], items:List[A]) extends AvroComplexValue[S, A]
  final case class AvroMapValue[S, A](schema:AvroMapType[S], values:Map[String, A]) extends AvroComplexValue[S, A]
  final case class AvroUnionValue[S, A](schema:AvroUnionType[S], member:A) extends AvroComplexValue[S, A]
  final case class AvroFixedValue[S, A](schema:AvroFixedType[S], bytes:Vector[Byte]) extends AvroComplexValue[S, A]





  sealed trait AvroSchema
  final case class AvroSchemaReference(name:String) extends AvroSchema
  final case class AvroSchemaUnion() extends AvroSchema
  final case class AvroSchemaType[A](aType:A) extends AvroSchema

}
