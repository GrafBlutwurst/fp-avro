name := "fp-avro"
version := "0.0.1-SNAPSHOT"

scalaVersion := "2.12.4"

scalacOptions ++= Seq(
  "-encoding", "UTF-8",   // source files are in UTF-8
  "-deprecation",         // warn about use of deprecated APIs
  "-unchecked",           // warn about unchecked type parameters
  "-feature",             // warn about misused language features
  "-language:higherKinds",// allow higher kinded types without `import scala.language.higherKinds`
  "-Xlint",               // enable handy linter warnings
  //"-Xfatal-warnings",     // turn compiler warnings into errors NEEDS TO BE DISABLED FOR SCALAFIX TO DO IT'S WORK
  "-Ypartial-unification", // allow the compiler to unify type constructors of different arities
  "-Ywarn-dead-code",                  // Warn when dead code is identified.
  "-Ywarn-extra-implicit",             // Warn when more than one implicit parameter section is defined.
  "-Ywarn-inaccessible",               // Warn about inaccessible types in method signatures.
  "-Ywarn-infer-any",                  // Warn when a type argument is inferred to be `Any`.
  "-Ywarn-nullary-override",           // Warn when non-nullary `def f()' overrides nullary `def f'.
  "-Ywarn-nullary-unit",               // Warn when nullary methods return Unit.
  "-Ywarn-numeric-widen",              // Warn when numerics are widened.
  "-Ywarn-unused:implicits",           // Warn if an implicit parameter is unused.
  "-Ywarn-unused:imports",             // Warn if an import selector is not referenced.
  "-Ywarn-unused:locals",              // Warn if a local definition is unused.
  "-Ywarn-unused:params",              // Warn if a value parameter is unused.
  "-Ywarn-unused:patvars",             // Warn if a variable bound in a pattern is unused.
  "-Ywarn-unused:privates",            // Warn if a private member is unused.
  "-Ywarn-value-discard",              // Warn when non-Unit expression results are unused.
  "-Ywarn-adapted-args"                // Warn on Autotupling
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

//Scala Dependencies
libraryDependencies ++= Seq(
  "eu.timepit"    %% "refined"         % "0.9.0",
  "com.slamdata"  %% "matryoshka-core" % "0.18.3",
  "com.chuusai"   %% "shapeless"       % "2.3.3"
)

//test Dependencies
libraryDependencies ++= Seq(
  "org.scalacheck" %% "scalacheck"         % "1.14.0",
  "eu.timepit"     %% "refined-scalacheck" % "0.9.0"
).map( _ %  "test" )

//Java Dependencies
libraryDependencies ++= Seq(
  "org.apache.avro" % "avro" % "1.8.2"
)


enablePlugins(TutPlugin)

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.6")
