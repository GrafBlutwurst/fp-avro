package com.scigility.exec

import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.Executors

import cats.Monad
import cats.effect._
import cats.implicits._
import com.scigility.fp_avro.Data._
import com.scigility.fp_avro.{implicits => _, _}
import matryoshka._
import matryoshka.implicits._
import matryoshka.data.Fix
import matryoshka.patterns.EnvT
import org.apache.hadoop.hbase.util.Bytes
import scalaz.Scalaz._
import com.scigility.fp_avro.implicits._
import scalaz._
import scodec.bits.ByteVector
import spinoco.fs2.kafka
import spinoco.protocol.kafka._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.string._

import scala.concurrent.ExecutionContext
import fs2.Scheduler
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

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
      case AvroEnumValue(schema, symbol)                  => Map(schema.namespace.value + "." + schema.name.value -> ByteVector(Bytes.toBytes(symbol))).right[String]
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
      case AvroFixedValue(schema, bytes)                  => Map(schema.namespace.value + "." + schema.name.value -> ByteVector(bytes)).right[String]

      case AvroRecordValue(schema, fields)                => Traverse[List].traverse(fields.toList)(
        (fld :(String, Cofree[AvroValue[Fix[AvroType], ?],String \/ Map[String,ByteVector]]) )=> {
          val localKey = s"""${schema.namespace}.${schema.name}.${fld._1}"""
          val value:String \/ Map[String,ByteVector] = fld._2.head

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
              kv => HBaseEntry("meta", kv._1, kv._2)
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
                                                                          HA:HBaseAlgebra[F],
                                                                          SA:SchemaRegistryAlgebra[F],
                                                                          KA:KafkaMessageAlgebra[F],
                                                                          AA: AvroAlgebra[F],
                                                                          LA: SelfAwareStructuredLogger[F]
  ) =Scheduler[F](corePoolSize = 1).flatMap( implicit s =>  {
    kafka
       .client(
         Set(kafka.broker("localhost", port = 9092)),
         ProtocolVersion.Kafka_0_10_2,
         "my-client-name"
       )
       .flatMap(kafkaClient => kafkaClient.subscribe(kafka.topic("lambdaletest"), kafka.partition(0), kafka.HeadOffset))
      .evalMap(topicMessage =>
         {
           val out = for {
             jsonAvroMsg <- KA.readJsonAvroMessage(topicMessage.message)
             _ <- LA.debug("parsed kafka message " + jsonAvroMsg)
             schemaString <- SA.getSchemaStringForID(jsonAvroMsg.schemaId)
             _ <- LA.debug("retrieved Schemastring " + schemaString)
             avroSchema <- AA.parseAvroSchema(schemaString)
             _ <- LA.debug("parsed Schema")
             typedSchema <- AA.unfoldAvroSchema[Fix](avroSchema)
             _ <- LA.debug("unfolded Schema")
             genRepr <- AA.decodeAvroJsonRepr(avroSchema)(jsonAvroMsg.payload)
             _ <- LA.debug("unfolded message")
             typedRepr <- AA.unfoldGenericRepr[Fix](typedSchema)(genRepr)
             _ <- LA.debug("refolded message to hbase repr")
           } yield foldTypedRepr(typedRepr, "value")

           out.recoverWith {
             case t => ConcurrentEffect[F].delay { t.getMessage.left[HBaseRow] }
           }
         }
       )
       .observe(_.collect { case -\/(err) => err }.to(fs2.Sink(s => LA.error(s))) )
       .observe(
         _.collect {
           case \/-(hbaseEntry) => hbaseEntry
         }.to(
           fs2.Sink(
               LA.info(s"WRITING TO HBASE") *>
               HA.write("smoketest", _)
           )
         )
       )
       .drain
  })


  def run(args:List[String]):IO[ExitCode] = {
    /**
      * This is the dummy logger just simply dumping on the console
      *
      **/
    implicit val loggerIO = new spinoco.fs2.kafka.Logger[IO] {
      def log(level: spinoco.fs2.kafka.Logger.Level.Value, msg: => String, throwable: Throwable): IO[Unit] = IO.apply(println(s"[$level]: $msg \t CAUSE: ${throwable.toString}"))
    }

    (
      HBaseAlgebra.effHBaseAlgebra[IO]("localhost:2181"),
      SchemaRegistryAlgebra.schemaRegistryAlgConc[IO]("http://localhost:8081"),
      Slf4jLogger.create[IO]
    ).mapN(
      (hbaseAlgebra, schemaRegistryAlgebra, logger) =>
        runStream[IO](
          hbaseAlgebra,
          schemaRegistryAlgebra,
          KafkaMessageAlgebra.effKafkaAlgebra[IO](";"),
          AvroAlgebra.catsMeInstance[Throwable, IO](errS => new RuntimeException(errS)),
          logger
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

    ).flatten




  }

}
