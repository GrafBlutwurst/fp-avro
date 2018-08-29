package com.scigility.exec

import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.Executors

import cats._
import cats.effect._
import cats.implicits._
import ch.grafblutwurst.anglerfish.data.avro.AvroData._
import ch.grafblutwurst.anglerfish.data.avro.AvroJsonFAlgebras
import ch.grafblutwurst.anglerfish.data.avro.implicits._
import com.scigility.fp_avro._
import matryoshka._
import matryoshka.implicits._
import matryoshka.data._
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
import shims._

object KafkaToHbase extends IOApp {


  implicit val EC: ExecutionContext = ExecutionContext.global
  implicit val AG: AsynchronousChannelGroup = AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(8))
  implicit val T : Timer[IO] = IO.timer


  /**
    * The meat of the usecase. Fold down the Avro AST into a HBase row. This is only possible if it's a RecordType and only contains Primitive fields or Fields that are a Union of a primitive and Null (representing an option type)
  **/
  //FIXME: Rethink flattening strategy and use of histomorphism. right now this is broken as described below (only detects direct nesting of records)
  def foldTypedRepr[M[_]:MonadError[?[_], Throwable]](columnFamily:String, keyField:String)(tRepr:Fix[AvroValue[Nu[AvroType], ?]])(implicit typeBirec:Birecursive.Aux[Nu[AvroType], AvroType]):M[HBaseRow] = {

    val M = MonadError[M, Throwable]


    def prefixCellIdentifiers(prefix: String)(cells:Catenable[HBaseCell]) =
      cells.map(
        cell => cell.copy(
          cellIdentifier = prefix + cell.cellIdentifier
        )
      )





    /*val cellCombinationMonoid: Monoid[M[Catenable[HBaseCell]]] = new Monoid[M[Catenable[HBaseCell]]] {
      override def empty: M[Catenable[HBaseCell]] = M.pure(Monoid[Catenable[HBaseCell]].empty)

      override def combine(x: M[Catenable[HBaseCell]], y: M[Catenable[HBaseCell]]): M[Catenable[HBaseCell]] =
        (x,y).mapN(
          (lhs, rhs) =>
            if (lhs.map(_.cellIdentifier).toList.toSet.intersect(rhs.map(_.cellIdentifier).toList.toSet).isEmpty)
              M.pure(lhs |+| rhs)
            else
              M.raiseError[Catenable[HBaseCell]](new RuntimeException("Cannot have duplicate keys."))
        ).flatten


    }*/



    import com.scigility.fp_avro.HBaseAlgebra.BytesGeneration._

    def cell(identifier:String, value:ByteVector) = HBaseCell(columnFamily, identifier, value)


    val assembleHBaseCells  : GAlgebraM[
        (Fix[AvroValue[Nu[AvroType], ?]], ?),
        M,
        AvroValue[Nu[AvroType], ?],
        Catenable[HBaseCell]
      ] = {

      //TERMINAL SYMBOLS
      //Primitives and single values with NS and name are always valid
      case AvroNullValue(_)                               => M.pure(Catenable.singleton(cell("null", ByteVector.empty)))//Map("null" -> ByteVector.empty).right[String]
      case AvroBooleanValue(_, value)                     => M.pure(Catenable.singleton(cell("boolean", value.byteVector)))//Map("boolean" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroIntValue(_, value)                         => M.pure(Catenable.singleton(cell("int", value.byteVector)))//Map("int" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroLongValue(_, value)                        => M.pure(Catenable.singleton(cell("long", value.byteVector)))//Map("long" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroFloatValue(_ , value)                      => M.pure(Catenable.singleton(cell("float", value.byteVector)))//Map("float" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroDoubleValue(_, value)                      => M.pure(Catenable.singleton(cell("double", value.byteVector)))//Map("double" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroBytesValue(_ , value)                      => M.pure(Catenable.singleton(cell("bytes", value.byteVector)))//Map("bytes" -> ByteVector(value)).right[String]
      case AvroStringValue(_, value)                      => M.pure(Catenable.singleton(cell("string", value.byteVector)))//Map("string" -> ByteVector(Bytes.toBytes(value))).right[String]
      case AvroEnumValue(schema, symbol)                  => M.pure(Catenable.singleton(cell(schema.name.value , symbol.byteVector)))//Map(schema.name.value -> ByteVector(Bytes.toBytes(symbol))).right[String]
      //insert as is
      case AvroFixedValue(schema, bytes)                  => M.pure(Catenable.singleton(cell(schema.name.value , bytes.byteVector)))
      // for unions we need to check if they're only used to represent optionals [primitive, null]
      case AvroUnionValue(schema, (_, value)) => {
        val validUnion = schema.members.map(typeBirec.project(_)).foldLeft((0, true))(
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

        if (validUnion) M.pure(value) else M.raiseError(new RuntimeException("Was not a valid Union, only one primitive plus null allowed"))
      }
      //NON TERMINAL SYMBOLS
      //for arrays we need to prepend the index of the item to generate a column for each item
      case AvroArrayValue(_, items)                       => M.pure( 
        items
          .map(_._2)
          .zipWithIndex
          .map(
            tpl => {
              val arrayItemCells = tpl._1
              val idx = tpl._2

              prefixCellIdentifiers(s"Array[$idx].")(arrayItemCells)
            }
          )
          .combineAll
      )
          

      //for maps we prepend the key
      case AvroMapValue(_, valueMap)                      => M.pure( 
        valueMap
        .map(kv => kv._1 -> kv._2._2)
        .toList
        .map(
          tpl => prefixCellIdentifiers(tpl._1)(tpl._2)
        )
        .combineAll
      )




      case AvroRecordValue(schema, fields)                => fields
        .toList
        .traverse(
          fieldDefinition => {

            val checkForRecordAlg: Algebra[AvroType, Boolean] = {
              case  AvroArrayType(items)   => items
              case  AvroMapType(values)    => values
              case  AvroUnionType(members) => members.forall(identity)
              case  _:AvroRecursionType[_] => false
              case  _:AvroRecordType[_]    => false
              case  _:AvroType[_]          => true
            }
            val schemaNu = typeBirec.embed(fieldDefinition._2._1.unFix.schema)

            if (!typeBirec.cata(schemaNu)(checkForRecordAlg))
              M.raiseError[Catenable[HBaseCell]](new RuntimeException("Nested Record Detected. This is not allowed " + fieldDefinition._2._1.unFix.schema))
            else
              M.pure(prefixCellIdentifiers(s"${schema.name}.${fieldDefinition._1}.")(fieldDefinition._2._2))
          }
        ).map(_.combineAll)
    }



    for {
      prefixedEntries <- tRepr.paraM(assembleHBaseCells)
      rowKey <- prefixedEntries.toList.find(_.cellIdentifier == keyField).map(M.pure).getOrElse(M.raiseError(new RuntimeException(s"no Key field of name $keyField was found in record")))
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
                                                                          LA: SelfAwareStructuredLogger[F]
  ) =Scheduler[F](corePoolSize = 1).flatMap( implicit s =>  {
    kafka
       .client(
         Set(kafka.broker("localhost", port = 9092)),
         ProtocolVersion.Kafka_0_10_2,
         "my-client-name-2"
       )
       .flatMap(kafkaClient => kafkaClient.subscribe(kafka.topic("lambdaletest"), kafka.partition(0), kafka.HeadOffset))
      .evalMap(topicMessage =>
         {
           val out = for {
             jsonAvroMsg <- KA.readJsonAvroMessage(topicMessage.message)
             _ <- LA.debug("parsed kafka message " + jsonAvroMsg)
             schemaString <- SA.getSchemaStringForID(jsonAvroMsg.schemaId)
             _ <- LA.debug("retrieved Schemastring " + schemaString)
             avroSchema <- AvroJsonFAlgebras.parseSchema[F](schemaString)
             _ <- LA.debug("unfolded Schema")
             typedRepr <- AvroJsonFAlgebras.parseDatum[F, Fix](avroSchema)(jsonAvroMsg.payload)
             _ <- LA.debug("refolded message to hbase repr")
             refolded <-foldTypedRepr[F]("meta", "foo.b.int")(typedRepr)
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
