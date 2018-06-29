package com
package scigility
package fp_avro


import cats.{ Monad, MonadError }
import scalaz._
import Scalaz._
import fs2._
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scodec.bits.ByteVector
import spinoco.fs2.kafka
import spinoco.fs2._
import spinoco.protocol.kafka._
import scala.concurrent.duration._
import cats.effect._
import cats.implicits._


import matryoshka._
import matryoshka.implicits._
import matryoshka.data.Fix
import implicits._
import Data._


object KafkaToHbase extends IOApp {


  implicit val EC: ExecutionContext = ExecutionContext.global
  implicit val S: Scheduler =  fs2.Scheduler.fromScheduledExecutorService(Executors.newScheduledThreadPool(4))
  implicit val AG: AsynchronousChannelGroup = AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(8))

  final case class HBaseEntry(columnFamily:String, columnIdentifier:String, value:ByteVector)
  final case class HBaseRow(rowKey:ByteVector, cells: IList[HBaseEntry])

  final case class JsonAvroMessage(schemaId:Int, payLoad:String)

  trait KafkaAlgebra[F[_]] {
    def readJsonAvroMessage(bytes:ByteVector):F[JsonAvroMessage]
  }

  trait HBaseAlgebra[F[_]]{
    def write(tableName:String, row: HBaseRow):F[Unit]
  }


  trait SchemaRegistryAlgebra[F[_]] {
    def retrieveSchemaForID(schemaId:Int):F[String]
  }

  def foldTypedRepr(tRepr:Fix[AvroValue[Fix[AvroType], ?]]):Either[String, HBaseEntry] = ???

  def runStream[F[_] : Effect : kafka.Logger : Monad ](
    HA:HBaseAlgebra[F], SA:SchemaRegistryAlgebra[F], KA:KafkaAlgebra[F], AA: AvroAlgebra[F]
  ) = {
    kafka.client(
      ensemble = Set(kafka.broker("kafka-broker1-dns-name", port = 9092))
        , protocol = ProtocolVersion.Kafka_0_10_2
        , clientName = "my-client-name"
    ).flatMap(kafkaClient => kafkaClient.subscribe(kafka.topic(???), kafka.partition(???), kafka.HeadOffset))
     .map(topicMessage => 
       {
         for {
           jsonAvroMsg <- KA.readJsonAvroMessage(topicMessage.message)
           schemaString <- SA.retrieveSchemaForID(jsonAvroMsg.schemaId)
           avroSchema <- AA.parseAvroSchema(schemaString)
           typedSchema <- AA.unfoldAvroSchema[Fix](avroSchema)
           genRepr <- AA.decodeAvroJsonRepr(avroSchema)(jsonAvroMsg.payLoad)
           typedRepr <- AA.unfoldGenericRepr[Fix](typedSchema)(genRepr)
         } yield foldTypedRepr(typedRepr)
       }
     )


    ???
  }


  def run(args:List[String]):IO[ExitCode] = {
    
  }

}
