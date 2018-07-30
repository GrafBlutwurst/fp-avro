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
import org.apache.hadoop.hbase.util.Bytes
import scodec.bits.ByteVector
import implicits._
import matryoshka.patterns.EnvT
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory

object KafkaToHbase extends IOApp {


  implicit val EC: ExecutionContext = ExecutionContext.global
  implicit val AG: AsynchronousChannelGroup = AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(8))
  implicit val T : Timer[IO] = IO.timer

  /**
    * The meat of the usecase. Fold down the Avro AST into a HBase row. This is only possible if it's a RecordType and only contains Primitive fields or Fields that are a Union of a primitive and Null (representing an option type)
  **/
  //FIXME: Rethink flattening strategy and use of histomorphism. right now this is broken as described below (only detects direct nesting of records)
  def foldTypedRepr(tRepr:Fix[AvroValue[Fix[AvroType], ?]], keyField:String):String \/ HBaseRow = {

    def tryMergeMap[K,V](lst:List[Map[K,V]]): String \/ Map[K,V] =
      lst.foldLeft[String \/ Map[K,V]](Map.empty[K, V].right[String])(
        (either, curr) => {
          either.flatMap(
            mapAcc => {
              val overlapping = (mapAcc.keySet union curr.keySet)
              if ( overlapping.isEmpty ) (mapAcc ++ curr).right[String] else s"Duplicate Keys detected $overlapping".left[Map[K,V] ]
            }
          )

        }
      )


    val alg  : GAlgebra[
        Cofree[AvroValue[Fix[AvroType], ?], ?],
        AvroValue[Fix[AvroType], ?],
        String \/ Map[String,ByteVector]
      ] = {
      //Primitives and single values with NS and name are always valid
      case AvroNullValue(_)                               => Map("value" -> ByteVector.empty).right[String]
      case AvroBooleanValue(_, value)                     => Map("value" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroIntValue(_, value)                         => Map("value" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroLongValue(_, value)                        => Map("value" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroFloatValue(_ , value)                      => Map("value" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroDoubleValue(_, value)                      => Map("value" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroBytesValue(_ , value)                      => Map("value" -> ByteVector(value)).right[String]
      case AvroStringValue(_, value)                      => Map("value" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroEnumValue(schema, symbol)                  => Map(schema.namespace + "." + schema.name -> ByteVector(Bytes.toBytes(symbol))).right[String]
      //for arrays we need to prepend the index of the item to generate a column for each item
      case AvroArrayValue(_, items)                       => Traverse[List].traverse(items) {
        case Cofree(value, _) => value.map(
          _.toList.zipWithIndex.map(
            withIndex => {
              s"Array[${withIndex._2}].${withIndex._1._1}" -> withIndex._1._2
            }
          ).toMap
        )
      }.flatMap(tryMergeMap)
      //for maps we prepend the key
      case AvroMapValue(_, valueMap)                      => Traverse[List].traverse(valueMap.toList) {
        case (k, Cofree(value, _)) => value.map(
          _.toList.map(
            kv => {
              s"$k.${kv._1}" -> kv._2
            }
          ).toMap
        )
      }.flatMap(tryMergeMap)
      // for unions we need to check if they're only used to represent optionals [primitive, null]
      case AvroUnionValue(schema, Cofree(value, _)) => {
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

        if (validUnion) value else "Was not a valid Union, only one primitive plus null allowed".left[Map[String,ByteVector]]
      }
        //insert as is
      case AvroFixedValue(schema, bytes)                  => Map(schema.namespace + "." + schema.name -> ByteVector(bytes)).right[String]

      case AvroRecordValue(schema, fields)                => Traverse[List].traverse(fields.toList)(
        (fld :(String, Cofree[AvroValue[Fix[AvroType], ?],String \/ Map[String,ByteVector]]) )=> {
          val localKey = s"""${schema.namespace}.${schema.name}.${fld._1}"""
          val value:String \/ Map[String,ByteVector] = fld._2.head
          //val hist:AvroValue[Fix[AvroType], Cofree[AvroValue[Fix[AvroType],?], String \/ Map[String,ByteVector]]] = fld._2.tail

          val checkForRecordAlg: Algebra[EnvT[String \/ Map[String,ByteVector], AvroValue[Fix[AvroType], ?], ?], Boolean] = {
            case EnvT((_, AvroNullValue(_)))           => true
            case EnvT((_, AvroBooleanValue(_, _)))     => true
            case EnvT((_, AvroIntValue(_, _)))         => true
            case EnvT((_, AvroLongValue(_, _)))        => true
            case EnvT((_, AvroFloatValue(_ , _)))      => true
            case EnvT((_, AvroDoubleValue(_, _)))      => true
            case EnvT((_, AvroBytesValue(_ , _)))      => true
            case EnvT((_, AvroStringValue(_, _)))      => true
            case EnvT((_, AvroEnumValue(_, _)))        => true
            case EnvT((_, AvroArrayValue(_, items)))   => items.all(identity)
            case EnvT((_, AvroMapValue(_, valueMap)))  => valueMap.values.toList.all(identity)
            case EnvT((_, AvroUnionValue(_, value)))   => value
            case EnvT((_, AvroFixedValue(_, _)))       => true
            case EnvT((_, AvroRecordValue(_, _)))      => false
          }

          val containsRecord = fld._2.cata(checkForRecordAlg)

          if (containsRecord)
            "Nested Record Detected. This is not allowed".left[Map[String,ByteVector]] //will not detect indirectly nested records
          else
            value.map(
              map => map.map(
                kv => s"$localKey.{kv._1}" -> kv._2
              )
            )

        }
      ).flatMap(tryMergeMap)
    }


    val eMap:String \/ Map[String,ByteVector] = tRepr.histo(alg)

    eMap.flatMap(
      map => {
        map.get(keyField).map(
          kf => {
            val cells = IList.fromList((map - keyField).toList).map(
              kv => HBaseEntry("columns", kv._1, kv._2)
            )

            HBaseRow(kf, cells)
          }
        ).fold[String \/ HBaseRow](
          s"Keyfield $keyField was not in keys ${map.keySet}".left[HBaseRow]
        )(
          _.right[String]
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
       .flatMap(kafkaClient => kafkaClient.subscribe(kafka.topic("testtopic"), kafka.partition(0), kafka.HeadOffset))
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
       .observe(_.collect { case -\/(err) => err }.to(fs2.Sink(s => Effect[F].delay(println(s)))) )
       .observe(_.collect { case \/-(hbaseEntry) => hbaseEntry }.to(fs2.Sink(HA.write("testTable", _))) )
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

    val conf : Configuration = HBaseConfiguration.create()
    val ZOOKEEPER_QUORUM = "localhost:2181"
    conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM)

    val connection = ConnectionFactory.createConnection(conf)

    runStream[IO](
      HBaseAlgebra.ioHBaseAlgebra(connection),
      SchemaRegistryAlgebra.ioSchemaRegistryAlrebra("localhost:8088/"),
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
