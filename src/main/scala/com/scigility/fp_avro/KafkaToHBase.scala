package com
package scigility
package fp_avro


import cats.{ Monad, MonadError }
import scalaz.{Scalaz, IList}
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

  /**
    * This is the dummy logger just simply dumping on the console
    * 
  **/
  implicit val loggerIO = new spinoco.fs2.kafka.Logger[IO] {
    def log(level: spinoco.fs2.kafka.Logger.Level.Value, msg: => String, throwable: Throwable): IO[Unit] = IO.apply(println(s"[$level]: $msg \t CAUSE: ${throwable.toString}"))
  }


  /**
    * This is the Algebra we require for Kafka Message handling. we assume the following format '{ "schemaID":0, "payload":{...} }'
  **/
  trait KafkaAlgebra[F[_]] {
    def readJsonAvroMessage(bytes:ByteVector):F[JsonAvroMessage]
  }

  /**
    * The only operation we require for HBase is writing a record to a table
  **/
  trait HBaseAlgebra[F[_]]{
    def write(tableName:String, row: HBaseRow):F[Unit]
  }

  /**
    * All we need here is retrieving a schema by ID. we assume the schema is already in the registry or else emmit an error
  **/
  trait SchemaRegistryAlgebra[F[_]] {
    def retrieveSchemaForID(schemaId:Int):F[String]
  }

  /**
    * The meat of the usecase. Fold down the Avro AST into a HBase row. This is only possible if it's a RecordType and only contains Primitive fields or Fields that are a Union of a primitive and Null (representing an option type)
  **/
  def foldTypedRepr(tRepr:Fix[AvroValue[Fix[AvroType], ?]], keyField:String):Either[String, HBaseRow] = ???


  /**
    * Read from Kafka
    * Decode the byte record into the expected Json envelope format
    * Retrieve the Schema from the registry
    * Unfold the schema into Avro AST representation
    * Unfold the payload into Avro GenRepr AST 
    * Fold the genrepr into HBase Row if possible
    * Write to Hbase if everything was a success. else write error to std out
  **/
  def runStream[F[_] : Effect : kafka.Logger : Monad ](
    HA:HBaseAlgebra[F], SA:SchemaRegistryAlgebra[F], KA:KafkaAlgebra[F], AA: AvroAlgebra[F]
  ) = {
    kafka
       .client(
         Set(kafka.broker("kafka-broker1-dns-name", port = 9092)),
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
             genRepr <- AA.decodeAvroJsonRepr(avroSchema)(jsonAvroMsg.payLoad)
             typedRepr <- AA.unfoldGenericRepr[Fix](typedSchema)(genRepr)
           } yield foldTypedRepr(typedRepr, "key")
         }
       )
       .observe(_.collect { case Left(err) => err }.to(Sink(s => Effect[F].delay(println(s)))) )
       .observe(_.collect { case Right(hbaseEntry) => hbaseEntry }.to(Sink(HA.write("testTable", _))) )
       .drain
  }


  def run(args:List[String]):IO[ExitCode] = {
    runStream[IO](???, ???, ???, ???)
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
