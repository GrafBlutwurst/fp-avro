package com
package scigility
package fp_avro

import eu.timepit.refined.api.Refined
import eu.timepit.refined.api.Validate

object Data{

  //Required Refinements
  final case class AvroValidUnion()

  /**
    * This makes sure that a List of AvroTypes actually forms a valid Union as defined in
    *  https://avro.apache.org/docs/1.8.1/spec.html#Unions
  **/
  implicit val validateUnionMembers: Validate.Plain[List[AvroType], AvroValidUnion] = 
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
    )

  //Avro Types required to represent Schemata
  sealed trait AvroType

  sealed trait AvroPrimitiveType extends AvroType
  final case object AvroNull extends AvroPrimitiveType
  final case object AvroBoolean extends AvroPrimitiveType
  final case object AvroInt extends AvroPrimitiveType
  final case object AvroLong extends AvroPrimitiveType
  final case object AvroFloat extends AvroPrimitiveType
  final case object AvroDouble extends AvroPrimitiveType
  final case object AvroBytes extends AvroPrimitiveType
  final case object AvroString extends AvroPrimitiveType


  sealed trait AvroComplexType extends AvroType
  final case class AvroRecord(namespace:String, name:String, doc:String, aliases:List[String], fields:List[AvroRecordField]) extends AvroComplexType 
  final case class AvroEnum(namespace:String, name:String, doc:String, aliases:List[String], symbols:List[String]) extends AvroComplexType
  final case class AvroArray(items:AvroType) extends AvroComplexType
  final case class AvroMap(values:AvroType) extends AvroComplexType
  final case class AvroUnion(members:List[AvroType] Refined AvroValidUnion) extends AvroComplexType
  final case class AvroFixed(namespace: String, name:String, aliases:List[String], length:Int) extends AvroComplexType

  //helpers
  sealed trait AvroRecordSortOrder
  final case object ARSOAscending extends AvroRecordSortOrder
  final case object ARSODescending extends AvroRecordSortOrder
  final case object ARSOIgnore extends AvroRecordSortOrder

  final case class AvroRecordField(name:String, doc:String, aType:AvroType, default:String, order:AvroRecordSortOrder, alias:String) //FIXME: default should somehow have something to do with the Avro type? does Default work for complex types? e.g. a field that is itself a records? if so how is it represented? JSON encoding?




  

  sealed trait AvroSchema
  final case class AvroSchemaReference(name:String) extends AvroSchema
  final case class AvroSchemaUnion() extends AvroSchema
  final case class AvroSchemaType(aType:AvroType) extends AvroSchema

}
