package com
package scigility
package fp_avro

import cats.effect.IO
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put}
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

  def ioHBaseAlgebra(con:Connection) = new HBaseAlgebra[IO] {
    override def write(tableName: String, row: HBaseRow): IO[Unit] = IO {
      val put = row.cells.foldLeft(new Put(row.rowKey.toByteBuffer)) (
        (put, cell) => {
          put.addColumn(Bytes.toBytes(cell.columnFamily), Bytes.toBytes(cell.columnIdentifier), cell.value.toArray)
        }
      )

      con.getTable(TableName.valueOf(Bytes.toBytes(tableName))).put(put)
    }
  }


}