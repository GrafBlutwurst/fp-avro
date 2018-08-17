package com
package scigility
package fp_avro

import cats._
import cats.implicits._
import cats.effect.{Effect, IO}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import scalaz.IList
import scodec.bits.ByteVector


final case class HBaseEntry(columnFamily:String, columnIdentifier:String, value:ByteVector)
final case class HBaseRow(rowKey:ByteVector, cells: IList[HBaseEntry])

/**
  * The only operation we require for HBase is writing a record to a table
  **/
trait HBaseAlgebra[F[_]]{
  def write(tableName:String, row: HBaseRow):F[Unit]
}


object HBaseAlgebra{




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
              val put =
                if (row.cells.nonEmpty)
                  row.cells.foldLeft(new Put(row.rowKey.toByteBuffer)) (
                    (put, cell) => {
                      put.addColumn(Bytes.toBytes(cell.columnFamily), Bytes.toBytes(cell.columnIdentifier), cell.value.toArray)
                    }
                  )
                else {
                  val put = new Put(row.rowKey.toByteBuffer)
                  put.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("occuranceToken"), Bytes.toBytes(true))
                }

              //FIXME could probably cache tables
              val table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)))
              table.put(put)
            }
          }
        }

    )




}