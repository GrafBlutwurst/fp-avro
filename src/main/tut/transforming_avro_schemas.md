# Getting to the internal Schema representation


This is an example how you get to the internal Schema representaiton given you already have an org.apache.avro.Schema instance

```tut
import matryoshka.data.Fix
import org.apache.avro.Schema

import matryoshka._
import matryoshka.implicits._
import com.scigility.fp_avro.implicits._
import com.scigility.fp_avro.Data._
import com.scigility.fp_avro.AvroAlgebra


val schemaString = """{     "type": "record",     "namespace": "com.example",     "name": "FullName",     "fields": [       { "name": "first", "type": "string" },       { "name": "last", "type": "string" }     ]} """
val schema:Schema = (new Schema.Parser).parse(schemaString)
    
val schemaInternal = schema.ana[Fix[AvroType]](AvroAlgebra.avroSchemaToInternalType)

println(schemaInternal)
    

```
