package com.scigility.exec

import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.Executors

import cats._
import cats.effect._
import cats.implicits._
import com.scigility.fp_avro.Data._
import com.scigility.fp_avro.{implicits => _, _}
import matryoshka._
import matryoshka.implicits._
import matryoshka.data._
import matryoshka.patterns.EnvT
import org.apache.hadoop.hbase.util.Bytes
import com.scigility.fp_avro.implicits._
import scodec.bits.ByteVector
import spinoco.fs2.kafka
import spinoco.protocol.kafka._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.string._

import scala.concurrent.ExecutionContext
import fs2.{Catenable, Scheduler}
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import scalaz.Cofree

object KafkaToHbase extends IOApp {


  implicit val EC: ExecutionContext = ExecutionContext.global
  implicit val AG: AsynchronousChannelGroup = AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(8))
  implicit val T : Timer[IO] = IO.timer


  /**
    * The meat of the usecase. Fold down the Avro AST into a HBase row. This is only possible if it's a RecordType and only contains Primitive fields or Fields that are a Union of a primitive and Null (representing an option type)
  **/
  //FIXME: Rethink flattening strategy and use of histomorphism. right now this is broken as described below (only detects direct nesting of records)
  def foldTypedRepr[F[_]:MonadError[?[_], Throwable]](columnFamily:String, keyField:String)(tRepr:Fix[AvroValue[Fix[AvroType], ?]])():F[HBaseRow] = {

    val F = MonadError[F, Throwable]


    def prefixCellIdentifiers(prefix: String)(cells:Catenable[HBaseCell]) =
      cells.map(
        cell => cell.copy(
          cellIdentifier = prefix + cell.cellIdentifier
        )
      )





    val cellCombinationMonoid: Monoid[F[Catenable[HBaseCell]]] = new Monoid[F[Catenable[HBaseCell]]] {
      override def empty: F[Catenable[HBaseCell]] = F.pure(Monoid[Catenable[HBaseCell]].empty)

      override def combine(x: F[Catenable[HBaseCell]], y: F[Catenable[HBaseCell]]): F[Catenable[HBaseCell]] =
        (x,y).mapN(
          (lhs, rhs) =>
            if (lhs.map(_.cellIdentifier).toList.toSet.intersect(rhs.map(_.cellIdentifier).toList.toSet).isEmpty)
              F.pure(lhs |+| rhs)
            else
              F.raiseError[Catenable[HBaseCell]](new RuntimeException("Cannot have duplicate keys."))
        ).flatten


    }



    import HBaseAlgebra.BytesGeneration._

    def cell(identifier:String, value:ByteVector) = HBaseCell(columnFamily, identifier, value)


    val assembleHBaseCells  : GAlgebra[
        Cofree[AvroValue[Fix[AvroType], ?], ?],
        AvroValue[Fix[AvroType], ?],
        F[Catenable[HBaseCell]]
      ] = {
      //Primitives and single values with NS and name are always valid
      case AvroNullValue(_)                               => F.pure(Catenable.singleton(cell("null", ByteVector.empty)))//Map("null" -> ByteVector.empty).right[String]
      case AvroBooleanValue(_, value)                     => F.pure(Catenable.singleton(cell("boolean", value.byteVector)))//Map("boolean" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroIntValue(_, value)                         => F.pure(Catenable.singleton(cell("int", value.byteVector)))//Map("int" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroLongValue(_, value)                        => F.pure(Catenable.singleton(cell("long", value.byteVector)))//Map("long" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroFloatValue(_ , value)                      => F.pure(Catenable.singleton(cell("float", value.byteVector)))//Map("float" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroDoubleValue(_, value)                      => F.pure(Catenable.singleton(cell("double", value.byteVector)))//Map("double" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroBytesValue(_ , value)                      => F.pure(Catenable.singleton(cell("bytes", value.byteVector)))//Map("bytes" -> ByteVector(value)).right[String]
      case AvroStringValue(_, value)                      => F.pure(Catenable.singleton(cell("string", value.byteVector)))//Map("string" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroEnumValue(schema, symbol)                  => F.pure(Catenable.singleton(cell(schema.name.value , symbol.byteVector)))//Map(schema.name.value -> ByteVector(Bytes.toBytes(symbol))).right[String]
      //for arrays we need to prepend the index of the item to generate a column for each item
      case AvroArrayValue(_, items)                       => items
          .map(_.head)
          .zipWithIndex
          .map(
            tpl => {
              val arrayItemCells = tpl._1
              val idx = tpl._2

              arrayItemCells.fmap(prefixCellIdentifiers(s"Array[$idx]."))
            }
          )
          .combineAll(cellCombinationMonoid)

      //for maps we prepend the key
      case AvroMapValue(_, valueMap)                      => valueMap
        .map(kv => kv._1 -> kv._2.head)
        .toList
        .map(
          tpl => tpl._2.fmap(prefixCellIdentifiers(tpl._1))
        )
        .combineAll(cellCombinationMonoid)

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

        if (validUnion) value else F.raiseError(new RuntimeException("Was not a valid Union, only one primitive plus null allowed"))
      }
        //insert as is
      case AvroFixedValue(schema, bytes)                  => F.pure(Catenable.singleton(cell(schema.name.value , bytes.byteVector)))

      case AvroRecordValue(schema, fields)                => fields
        .toList
        .map(
          fieldDefinition => {
            val checkForRecordAlg: Algebra[EnvT[F[Catenable[HBaseCell]], AvroValue[Fix[AvroType], ?], ?], Boolean] = {
              case EnvT((_, AvroNullValue(_)))           => true
              case EnvT((_, AvroBooleanValue(_, _)))     => true
              case EnvT((_, AvroIntValue(_, _)))         => true
              case EnvT((_, AvroLongValue(_, _)))        => true
              case EnvT((_, AvroFloatValue(_ , _)))      => true
              case EnvT((_, AvroDoubleValue(_, _)))      => true
              case EnvT((_, AvroBytesValue(_ , _)))      => true
              case EnvT((_, AvroStringValue(_, _)))      => true
              case EnvT((_, AvroEnumValue(_, _)))        => true
              case EnvT((_, AvroArrayValue(_, items)))   => items.forall(identity)
              case EnvT((_, AvroMapValue(_, valueMap)))  => valueMap.values.toList.forall(identity)
              case EnvT((_, AvroUnionValue(_, value)))   => value
              case EnvT((_, AvroFixedValue(_, _)))       => true
              case EnvT((_, AvroRecordValue(_, _)))      => false
            }

            if (fieldDefinition._2.cata(checkForRecordAlg))
              F.raiseError[Catenable[HBaseCell]](new RuntimeException("Nested Record Detected. This is not allowed"))
            else
              fieldDefinition._2.head.map(prefixCellIdentifiers(s"${schema.name}.${fieldDefinition._1}."))
          }
        ).combineAll(cellCombinationMonoid)
    }



    for {
      prefixedEntries <- tRepr.histo(assembleHBaseCells)
      rowKey <- prefixedEntries.toList.find(_.cellIdentifier == keyField).map(F.pure).getOrElse(F.raiseError(new RuntimeException(s"no Key field of name $keyField was found in record")))
    } yield HBaseRow(rowKey.value, prefixedEntries)
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
             refolded <-foldTypedRepr[F]("meta", "key")(typedRepr)
           } yield refolded

           out.attempt
         }
       )
       .observe(_.collect { case Left(err) => err }.to(fs2.Sink(t => LA.error(t)("Error during Processing Message:"))) )
       .observe(
         _.collect {
           case Right(hbaseEntry) => hbaseEntry
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
