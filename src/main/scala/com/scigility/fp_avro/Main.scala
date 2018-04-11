package com
package scigility
package fp_avro

import matryoshka.data.Fix
import org.apache.avro.Schema

import matryoshka._
import matryoshka.implicits._
import implicits._
import Data._

object Main{

  def main(args:Array[String]):Unit = {
    val schemaString = """{     "type": "record",     "namespace": "com.example",     "name": "FullName",     "fields": [       { "name": "first", "type": "string" },       { "name": "last", "type": "string" }     ]} """
    val schema:Schema = (new Schema.Parser).parse(schemaString)
    
    val schemaInternal = schema.ana[Fix[AvroType]](AvroAlgebra.avroSchemaToInternalType)

    println(schemaInternal)

  }

}
