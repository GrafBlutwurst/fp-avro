package com
package scigility
package fp_avro

import cats._
import cats.implicits._
import cats.effect.{Effect, IO}
import fs2.Catenable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import scodec.bits.ByteVector


final case class HBaseCell(columnFamily:String, cellIdentifier:String, value:ByteVector)
final case class HBaseRow(rowKey:ByteVector, cells: Catenable[HBaseCell])


/**
  * The only operation we require for HBase is writing a record to a table
  **/
trait HBaseAlgebra[F[_]]{
  def write(tableName:String, row: HBaseRow):F[Unit]
}


object HBaseAlgebra{


  object BytesGeneration {

    trait ByteVectorGenerator[T] {
      def toByteVector(t:T):ByteVector
    }

    implicit val bvgNull:ByteVectorGenerator[Null] = (_:Null) => ByteVector.empty
    implicit val bvgBoolean:ByteVectorGenerator[Boolean] = (value:Boolean) => ByteVector(Bytes.toBytes(value))
    implicit val bvgInt:ByteVectorGenerator[Int] = (value:Int) => ByteVector(Bytes.toBytes(value))
    implicit val bvgLong:ByteVectorGenerator[Long] = (value:Long) => ByteVector(Bytes.toBytes(value))
    implicit val bvgFloat:ByteVectorGenerator[Float] = (value:Float) => ByteVector(Bytes.toBytes(value))
    implicit val bvgDouble:ByteVectorGenerator[Double] = (value:Double) => ByteVector(Bytes.toBytes(value))
    implicit val bvgBytes:ByteVectorGenerator[Vector[Byte]] = (value:Vector[Byte]) => ByteVector(value)
    implicit val bvgString:ByteVectorGenerator[String] = (value:String) => ByteVector(Bytes.toBytes(value))


    implicit class toBytesSyntax[T : ByteVectorGenerator](t:T)(implicit bvg:ByteVectorGenerator[T]) {
      def byteVector:ByteVector = bvg.toByteVector(t)
    }

  }



  def effHBaseAlgebra[F[_]:Effect](zkQuorum:String): F[HBaseAlgebra[F]] =
    Effect[F].delay
      {
        val conf = HBaseConfiguration.create()
        val ZOOKEEPER_QUORUM = zkQuorum
        conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM)
        ConnectionFactory.createConnection(conf)
      }
      .map(
        connection => {
          new HBaseAlgebra[F] {
            override def write(tableName: String, row: HBaseRow): F[Unit] = Effect[F].delay {
              val put = row.cells.foldLeft(new Put(row.rowKey.toByteBuffer)) (
                    (put, cell) => {
                      put.addColumn(Bytes.toBytes(cell.columnFamily), Bytes.toBytes(cell.cellIdentifier), cell.value.toArray)
                    }
                  )

              //FIXME could probably cache tables
              val table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)))
              table.put(put)
            }
          }
        }

    )




}