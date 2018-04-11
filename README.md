# fp-avro
This library provides access to generic Avro records and schemata in scala. This is very much WIP

# Motivation
Working with Avro in a schema driven way in Scala has been quite a pain so far. Forcing you to directly interact with the org.apache.avro Java API. This meant some very heavy handed casting had to be done. 
The goal of fp-avro is to provide a principled and pure FP alternative to this using Recursion Schemes.

# When to use
TODO

# How to use
TODO

# Credits
TODO

# Wanna Help?
TODO

# Ideas and Upcoming stuff
So one Idea is being able to write Functions from (AvroSchema, AvroValue) => AvroValue and have the library infer the new schema. Basically being able to map over the an AvroRecord and deconstruct it and apply things like:
"If you come accross a field with the name 'foo' on a Record and it has the type String, apply this function f: String => Int" and it'll output the new inferred schema as well as a function that transforms records.
