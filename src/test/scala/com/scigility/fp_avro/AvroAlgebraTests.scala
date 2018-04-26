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
import scala.collection.immutable.{ ListMap, ListSet }
import matryoshka._
import matryoshka.implicits._
import implicits._
import eu.timepit.refined.api.Refined
import scalaz._
import Scalaz._
import eu.timepit.refined._
import eu.timepit.refined.numeric._
import eu.timepit.refined.auto._


object AvroAlgebraTests extends Properties("AvroType"){

  val nameSpaceGen = oneOf(
    const(refineMV[AvroValidNamespace]("com.scigility")),
    const(refineMV[AvroValidNamespace]("asdf.ghjk.yxcv")),
    const(refineMV[AvroValidNamespace]("foo.bar.baz.rar"))
  )

  val nameGen = oneOf(
    const(refineMV[AvroValidName]("foo")),
    const(refineMV[AvroValidName]("bar")),
    const(refineMV[AvroValidName]("baz")),
    const(refineMV[AvroValidName]("baz1")),
    const(refineMV[AvroValidName]("baz2")),
    const(refineMV[AvroValidName]("baz3"))
  )


  def avroRecordFieldMetaDataGen[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]) = for {
    name <- nameGen
    doc <- arbitrary[Option[String]]
    sortOrder <- oneOf(Some(ARSOIgnore), Some(ARSOAscending), Some(ARSODescending), Option.empty[AvroRecordSortOrder])
    aliases <- arbitrary[Option[Set[String]]]
  } yield AvroRecordFieldMetaData(name, doc, Option.empty[String], sortOrder, aliases)

  def fldGen[F[_[_]]](depth:Int)(implicit birec:Birecursive.Aux[F[AvroType], AvroType]) = for {
    md <- avroRecordFieldMetaDataGen
    fs <- avroTypeGen[F](depth+1)
  } yield (md, fs)

  val fldSize = 10
  def avroRecordTypeGen[F[_[_]]](depth:Int)(implicit birec:Birecursive.Aux[F[AvroType], AvroType]) = for {
    nameSpace <- nameSpaceGen
    name <- nameGen
    doc <- arbitrary[Option[String]]
    aliases <- arbitrary[Option[Set[String]]]
    flds <- Gen.listOfN(fldSize, fldGen(depth)).map(_.foldLeft(ListMap.empty[AvroRecordFieldMetaData,F[AvroType]])(_ + _))
  } yield birec.embed(AvroRecordType(nameSpace, name, doc, aliases, flds))

  val enumSymbolSize = 5
  def avroEnumTypeGen[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]) = for {
    nameSpace <- nameSpaceGen
    name <- nameGen
    doc <- arbitrary[Option[String]]
    aliases <- arbitrary[Option[Set[String]]]
    symbols <- Gen.listOfN(enumSymbolSize, nameGen).map(lst => lst.foldLeft(ListSet.empty[String Refined AvroValidName])( (ls, elem) => ls + elem))
  } yield birec.embed(AvroEnumType(nameSpace, name, doc, aliases, symbols))


  def avroArrayTypeGen[F[_[_]]](depth:Int)(implicit birec:Birecursive.Aux[F[AvroType], AvroType]) = for {
    items <- avroTypeGen[F](depth+1)
  }  yield birec.embed(AvroArrayType(items))

  def avroMapTypeGen[F[_[_]]](depth:Int)(implicit birec:Birecursive.Aux[F[AvroType], AvroType]) = for {
    values <- avroTypeGen[F](depth +1 )
  }  yield birec.embed(AvroMapType(values))

  def avroUnionTypeGen[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]):Gen[F[AvroUnionType]] = ???

  def avroFixedTypeGen[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]) = for {
    nameSpace <- nameSpaceGen
    name <- nameGen
    doc <- arbitrary[Option[String]]
    aliases <- arbitrary[Option[Set[String]]]
    length <- arbitrary[Int Refined Positive]
  } yield birec.embed(AvroFixedType(nameSpace, name, doc, aliases, length))

  def avroTypeGen[F[_[_]]](depth:Int)(implicit birec:Birecursive.Aux[F[AvroType], AvroType]):Gen[F[AvroType]] = 
    if(depth < 5 )
      oneOf(
        const(birec.embed(AvroNullType[F[AvroType]])),
        const(birec.embed(AvroBooleanType[F[AvroType]])),
        const(birec.embed(AvroIntType[F[AvroType]])),
        const(birec.embed(AvroLongType[F[AvroType]])),
        const(birec.embed(AvroFloatType[F[AvroType]])),
        const(birec.embed(AvroDoubleType[F[AvroType]])),
        const(birec.embed(AvroBytesType[F[AvroType]])),
        const(birec.embed(AvroStringType[F[AvroType]])),
        avroRecordTypeGen(depth),
        avroEnumTypeGen,
        avroArrayTypeGen(depth),
        avroMapTypeGen(depth),
        avroFixedTypeGen
      )
    else 
      oneOf(
        const(birec.embed(AvroNullType[F[AvroType]])),
        const(birec.embed(AvroBooleanType[F[AvroType]])),
        const(birec.embed(AvroIntType[F[AvroType]])),
        const(birec.embed(AvroLongType[F[AvroType]])),
        const(birec.embed(AvroFloatType[F[AvroType]])),
        const(birec.embed(AvroDoubleType[F[AvroType]])),
        const(birec.embed(AvroBytesType[F[AvroType]])),
        const(birec.embed(AvroStringType[F[AvroType]])),        
        avroEnumTypeGen,
        avroFixedTypeGen
      )

  


  def identityProperty[F[_[_]]](implicit birec:Birecursive.Aux[F[AvroType], AvroType]) = forAll(avroTypeGen[F](0)) {
    (t:F[AvroType]) => birec.cataM(t)(AvroAlgebra.avroTypeToSchema).flatMap(_.anaM[F[AvroType]](AvroAlgebra.avroSchemaToInternalType)).fold(s => {println(s); false}, _ == t)
  }

  identityProperty[Fix].check
}
