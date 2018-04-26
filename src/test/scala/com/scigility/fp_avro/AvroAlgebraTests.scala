package com
package scigility
package fp_avro


import org.scalacheck._
import Gen._
import Arbitrary.arbitrary
import implicits._
import Data._
import org.scalacheck.Prop.forAll
import matryoshka.data.Fix
import scala.collection.immutable.ListMap
import matryoshka._
import matryoshka.implicits._
import implicits._

import scalaz._



object AvroAlgebraTests extends Properties("AvroType"){

  def avroRecordFieldMetaDataGen[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]) = for {
    name <- arbitrary[String]
    doc <- arbitrary[Option[String]]
    sortOrder <- oneOf(Some(ARSOIgnore), Some(ARSOAscending), Some(ARSODescending), Option.empty[AvroRecordSortOrder])
    aliases <- arbitrary[Option[Set[String]]]
  } yield AvroRecordFieldMetaData(name, doc, Option.empty[String], sortOrder, aliases)

  def fldGen[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]) = for {
    md <- avroRecordFieldMetaDataGen
    fs <- avroTypeGen[F]
  } yield (md, fs)

  val fldSize = 10
  def avroRecordTypeGen[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]) = for {
    nameSpace <- arbitrary[String]
    name <- arbitrary[String]
    doc <- arbitrary[Option[String]]
    aliases <- arbitrary[Option[Set[String]]]
    flds <- Gen.listOfN(fldSize, fldGen).map(_.foldLeft(ListMap.empty[AvroRecordFieldMetaData,F[AvroType]])(_ + _))
  } yield birec.embed(AvroRecordType(nameSpace, name, doc, aliases, flds))

  val enumSymbolSize = 5
  def avroEnumTypeGen[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]) = for {
    nameSpace <- arbitrary[String]
    name <- arbitrary[String]
    doc <- arbitrary[Option[String]]
    aliases <- arbitrary[Option[Set[String]]]
    symbols <- Gen.listOfN(enumSymbolSize, arbitrary[String])
  } yield birec.embed(AvroEnumType(nameSpace, name, doc, aliases, symbols))


  def avroArrayTypeGen[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]) = for {
    items <- avroTypeGen[F]
  }  yield birec.embed(AvroArrayType(items))

  def avroMapTypeGen[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]) = for {
    values <- avroTypeGen[F]
  }  yield birec.embed(AvroMapType(values))

  def avroUnionTypeGen[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]):Gen[F[AvroUnionType]] = ???

  def avroFixedTypeGen[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]) = for {
    nameSpace <- arbitrary[String]
    name <- arbitrary[String]
    doc <- arbitrary[Option[String]]
    aliases <- arbitrary[Option[Set[String]]]
    length <- arbitrary[Int]
  } yield birec.embed(AvroFixedType(nameSpace, name, doc, aliases, length))

  def avroTypeGen[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]):Gen[F[AvroType]] = 
    oneOf(
      const(birec.embed(AvroNullType[F[AvroType]])), 
      const(birec.embed(AvroBooleanType[F[AvroType]])), 
      const(birec.embed(AvroIntType[F[AvroType]])), 
      const(birec.embed(AvroLongType[F[AvroType]])), 
      const(birec.embed(AvroFloatType[F[AvroType]])), 
      const(birec.embed(AvroDoubleType[F[AvroType]])), 
      const(birec.embed(AvroBytesType[F[AvroType]])), 
      const(birec.embed(AvroStringType[F[AvroType]])), 
      avroRecordTypeGen, 
      avroEnumTypeGen, 
      avroArrayTypeGen, 
      avroMapTypeGen, 
      avroFixedTypeGen
    )
  


  def identityProperty[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]) = forAll(avroTypeGen[F]) {
    (t:F[AvroType]) => birec.cata(t)(AvroAlgebra.avroTypeToSchema).ana[F[AvroType]](AvroAlgebra.avroSchemaToInternalType) == t
  }

  identityProperty[Fix].check
}
