package com
package scigility
package fp_avro


import cats.Monad
import scalaz._
import Scalaz._
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext
import spinoco.fs2.kafka
import spinoco.fs2._
import spinoco.protocol.kafka._
import cats.effect._
import cats.implicits._
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import Data.{AvroValue, _}
import org.apache.avro.generic.GenericData
import org.apache.hadoop.hbase.util.Bytes
import scodec.bits.ByteVector
import implicits._

object KafkaToHbase extends IOApp {


  implicit val EC: ExecutionContext = ExecutionContext.global
  implicit val AG: AsynchronousChannelGroup = AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(8))
  implicit val T : Timer[IO] = IO.timer

  /**
    * The meat of the usecase. Fold down the Avro AST into a HBase row. This is only possible if it's a RecordType and only contains Primitive fields or Fields that are a Union of a primitive and Null (representing an option type)
  **/
  //FIXME: Rethink flattening strategy and use of histomorphism. right now this is broken as described below (only detects direct nesting of records)
  def foldTypedRepr(tRepr:Fix[AvroValue[Fix[AvroType], ?]], keyField:String):Either[String, HBaseRow] = {
    val alg  : GAlgebra[
        Cofree[AvroValue[Fix[AvroType], ?], ?],
        AvroValue[Fix[AvroType], ?],
        Either[String, Map[String,ByteVector]]
      ] = {
      //Primitives and single values with NS and name are always valid
      case AvroNullValue(_)                               => Right(Map("value" -> ByteVector.empty))
      case AvroBooleanValue(_, value)                     => Right(Map("value" -> ByteVector(Bytes.toBytes(value))))
      case AvroIntValue(_, value)                         => Right(Map("value" -> ByteVector(Bytes.toBytes(value))))
      case AvroLongValue(_, value)                        => Right(Map("value" -> ByteVector(Bytes.toBytes(value))))
      case AvroFloatValue(_ , value)                      => Right(Map("value" -> ByteVector(Bytes.toBytes(value))))
      case AvroDoubleValue(_, value)                      => Right(Map("value" -> ByteVector(Bytes.toBytes(value))))
      case AvroBytesValue(_ , value)                      => Right(Map("value" -> ByteVector(value)))
      case AvroStringValue(_, value)                      => Right(Map("value" -> ByteVector(Bytes.toBytes(value))))
      case AvroEnumValue(schema, symbol)                  => Right(Map(schema.namespace + "." + schema.name -> ByteVector(Bytes.toBytes(symbol))))
      //for arrays we need to prepend the index of the item to generate a column for each item
      case AvroArrayValue(_, items)                       => Traverse[List].traverse(items) {
        case Cofree(value, history) => value.map(
          _.toList.zipWithIndex.map(
            withIndex => {
              s"Array[${withIndex._2}].${withIndex._1._1}" -> withIndex._1._2
            }
          ).toMap
        )
      }.flatMap(
        lst => lst.foldLeft[Either[String, Map[String, ByteVector]]](Right(Map.empty[String, ByteVector]))(
          (either, curr) => {
            either.flatMap(
              mapAcc => {
                val overlapping = (mapAcc.keySet union curr.keySet)
                if ( overlapping.isEmpty ) Right(mapAcc ++ curr) else Left(s"Duplicate Keys detected $overlapping")
              }
            )

          }
        )
      )
      //for maps we prepend the key
      case AvroMapValue(_, valueMap)                      => Traverse[List].traverse(valueMap.toList) {
        case (k, Cofree(value, history)) => value.map(
          _.toList.map(
            kv => {
              s"$k.${kv._1}" -> kv._2
            }
          ).toMap
        )
      }.flatMap(
        lst => lst.foldLeft[Either[String, Map[String, ByteVector]]](Right(Map.empty[String, ByteVector]))(
          (either, curr) => {
            either.flatMap(
              mapAcc => {
                val overlapping = (mapAcc.keySet union curr.keySet)
                if ( overlapping.isEmpty ) Right(mapAcc ++ curr) else Left(s"Duplicate Keys detected $overlapping")
              }
            )

          }
        )
      )
      // for unions we need to check if they're only used to represent optionals [primitive, null]
      case AvroUnionValue(schema, Cofree(value, history)) => {
        val validUnion = schema.members.map(_.unFix).foldLeft((0, true))(
          (tpl, dt) => dt match {
            case AvroNullType()     => tpl
            case AvroBooleanType()  => if (tpl._1 == 0) (tpl._1 + 1, false) else (tpl._1 + 1 , tpl._2)
            case AvroIntType()      => if (tpl._1 == 0) (tpl._1 + 1, false) else (tpl._1 + 1 , tpl._2)
            case AvroLongType()     => if (tpl._1 == 0) (tpl._1 + 1, false) else (tpl._1 + 1 , tpl._2)
            case AvroFloatType()    => if (tpl._1 == 0) (tpl._1 + 1, false) else (tpl._1 + 1 , tpl._2)
            case AvroDoubleType()   => if (tpl._1 == 0) (tpl._1 + 1, false) else (tpl._1 + 1 , tpl._2)
            case AvroBytesType()    => if (tpl._1 == 0) (tpl._1 + 1, false) else (tpl._1 + 1 , tpl._2)
            case AvroStringType()   => if (tpl._1 == 0) (tpl._1 + 1, false) else (tpl._1 + 1 , tpl._2)
            case _ => (tpl._1 + 1, false)
          }
        )._2

        if (validUnion) value else Left("Was not a valid Union, only one primitive plus null allowed")
      }
        //insert as is
      case AvroFixedValue(schema, bytes)                  => Right(Map(schema.namespace + "." + schema.name -> ByteVector(bytes)))

      case AvroRecordValue(schema, fields)                => Traverse[List].traverse(fields.toList)(
        (fld :(String, Cofree[AvroValue[Fix[AvroType], ?],Either[String, Map[String,ByteVector]]]) )=> {
          val localKey = s"""${schema.namespace}.${schema.name}.${fld._1}"""
          val value:Either[String, Map[String,ByteVector]] = fld._2.head
          val hist:AvroValue[Fix[AvroType], Cofree[AvroValue[Fix[AvroType],?], Either[String, Map[String,ByteVector]]]] = fld._2.tail

          hist match {
            case AvroRecordValue(_, _) => Left("Nested Record Detected. This is not allowed") //will not detect indirectly nested records
            case _ => value.map(
              map => map.map(
                kv => s"$localKey.{kv._1}" -> kv._2
              )
            )
          }
        }
      ).flatMap(
        lst => lst.foldLeft[Either[String, Map[String, ByteVector]]](Right(Map.empty[String, ByteVector]))(
          (either, curr) => {
            either.flatMap(
              mapAcc => {
                val overlapping = (mapAcc.keySet union curr.keySet)
                if ( overlapping.isEmpty ) Right(mapAcc ++ curr) else Left(s"Duplicate Keys detected $overlapping")
              }
            )

          }
        )
      )
    }


    val eMap:Either[String, Map[String,ByteVector]] = tRepr.histo(alg)

    eMap.flatMap(
      map => {
        map.get(keyField).map(
          kf => {
            val cells = IList.fromList((map - keyField).toList).map(
              kv => HBaseEntry("columns", kv._1, kv._2)
            )

            HBaseRow(kf, cells)
          }
        ).fold[Either[String, HBaseRow]](
          Left(s"Keyfield $keyField was not in keys ${map.keySet}")
        )(
          Right(_)
        )
      }
    )
  }



  /**
    * Read from Kafka
    * Decode the byte record into the expected Json envelope format
    * Retrieve the Schema from the registry
    * Unfold the schema into Avro AST representation
    * Unfold the payload into Avro GenRepr AST 
    * Fold the genrepr into HBase Row if possible
    * Write to Hbase if everything was a success. else write error to std out
  **/
  def runStream[F[_] : ConcurrentEffect : kafka.Logger : Monad : Timer ](
    HA:HBaseAlgebra[F], SA:SchemaRegistryAlgebra[F], KA:KafkaAlgebra[F], AA: AvroAlgebra[F]
  ) = {
    kafka
       .client(
         Set(kafka.broker("localhost", port = 9092)),
         ProtocolVersion.Kafka_0_10_2,
         "my-client-name"
       )
       .flatMap(kafkaClient => kafkaClient.subscribe(kafka.topic(???), kafka.partition(???), kafka.HeadOffset))
      .evalMap(topicMessage =>
         {
           for {
             jsonAvroMsg <- KA.readJsonAvroMessage(topicMessage.message)
             schemaString <- SA.retrieveSchemaForID(jsonAvroMsg.schemaId)
             avroSchema <- AA.parseAvroSchema(schemaString)
             typedSchema <- AA.unfoldAvroSchema[Fix](avroSchema)
             genRepr <- AA.decodeAvroJsonRepr(avroSchema)(jsonAvroMsg.payload)
             typedRepr <- AA.unfoldGenericRepr[Fix](typedSchema)(genRepr)
           } yield foldTypedRepr(typedRepr, "key")
         }
       )
       .observe(_.collect { case Left(err) => err }.to(fs2.Sink(s => Effect[F].delay(println(s)))) )
       .observe(_.collect { case Right(hbaseEntry) => hbaseEntry }.to(fs2.Sink(HA.write("testTable", _))) )
       .drain
  }


  def run(args:List[String]):IO[ExitCode] = {
    /**
      * This is the dummy logger just simply dumping on the console
      *
      **/
    implicit val loggerIO = new spinoco.fs2.kafka.Logger[IO] {
      def log(level: spinoco.fs2.kafka.Logger.Level.Value, msg: => String, throwable: Throwable): IO[Unit] = IO.apply(println(s"[$level]: $msg \t CAUSE: ${throwable.toString}"))
    }


    val eitherIONT = new (\/[String, ?] ~> IO) {
      override def apply[A](fa: String \/ A): IO[A] = fa.fold(
        err => IO.raiseError(new RuntimeException(err.toString)),
        v => IO.pure(v))
    }

    runStream[IO](
      HBaseAlgebra.ioHBaseAlgebra(???),
      SchemaRegistryAlgebra.ioSchemaRegistryAlrebra(???),
      KafkaAlgebra.ntAlgebra(KafkaAlgebra.eKafkaAlgebra)(eitherIONT),
      AvroAlgebra.catsMeInstance[Throwable, IO](errS => new RuntimeException(errS))
    )
      .compile
      .drain
      .attempt
      .map {
        case Right(_) => ExitCode.Success
        case Left(throwable) =>  {
          println(throwable.toString)
          ExitCode(-1)
        }
      }
  }

}
