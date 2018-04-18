package com
package scigility
package fp_avro

import java.io.File
import matryoshka.data.Fix
import org.apache.avro.Schema

import matryoshka._
import matryoshka.implicits._
import implicits._
import Data._
import org.apache.avro.file.{ DataFileReader, DataFileWriter }
import org.apache.avro.generic.{ GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord }
import scalaz._
import Scalaz._


object Main{

  def main(args:Array[String]):Unit = {
    val schemaString = """{     "type": "record",     "namespace": "com.example",     "name": "FullName",     "fields": [       { "name": "first", "type": "string" },       { "name": "last", "type": "string" }, {"name": "uniontest", "type": ["null","string","int"]}     ]} """
    val schema:Schema = (new Schema.Parser).parse(schemaString)
    
    val schemaInternal = schema.ana[Fix[AvroType]](AvroAlgebra.avroSchemaToInternalType)

    println(schemaInternal)


    val genRec = new GenericData.Record(schema)
    genRec.put("first", "ASDF")
    genRec.put("last", "FOO")
    genRec.put("uniontest", 4)

    val avroFile = new File("avro.avro")
    val datumWriter = new GenericDatumWriter[GenericData.Record](schema)
    val dataFileWriter = new DataFileWriter[GenericData.Record](datumWriter)
    dataFileWriter.create(schema, avroFile)
    dataFileWriter.append(genRec)
    dataFileWriter.close


    val datumReader = new GenericDatumReader[GenericData.Record](schema)
    val datafileReader = new DataFileReader[GenericData.Record](avroFile, datumReader)
    val deserializedGenRec = datafileReader.next

    println(deserializedGenRec.get("uniontest").getClass.getName)


    val pair:(Fix[AvroType], Any) = (schemaInternal, deserializedGenRec)
    val alg = AvroAlgebra.avroGenericReprToInternal[Fix]

    val out = pair.anaM[Fix[AvroValue[Fix[AvroType], ?]]](alg)
    println(out)

    val schemaC = schemaInternal.cata(AvroAlgebra.avroTypeToSchema)

    println(s"orig schema: $schema")
    println(s"cata schema: $schemaC")

    assert(schemaC.equals(schema))

    val recC = out.right.get.cata(AvroAlgebra.avroValueToGenericRepr[Fix])
    println(s"orig rec: $deserializedGenRec")
    println(s"cata rec: $recC")
    assert(recC.equals(deserializedGenRec))

  }

}

