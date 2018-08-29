name := "fp-avro"
version := "0.0.3-SNAPSHOT"

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

val circeVersion = "0.9.3"
val log4catsVersion = "0.1.0"
val http4sVersion = "0.18.16"

//Scala Dependencies
libraryDependencies ++= Seq(
  "ch.grafblutwurst" %% "anglerfish" %  "0.1.1",
  //"co.fs2"        %% "fs2-core"        % "1.0.0-M2", //implicitly in http4s
  "com.spinoco"   %% "fs2-kafka"       % "0.2.0",
  "org.typelevel" %% "cats-effect"     % "1.0.0-RC2",
  "org.http4s"        %% "http4s-dsl"                 % http4sVersion,
  "org.http4s"        %% "http4s-blaze-client"        % http4sVersion,
  "org.http4s"        %% "http4s-circe"               % http4sVersion,
  "org.http4s"        %% "http4s-core"               % http4sVersion,
  "io.circe"          %% "circe-generic"              % circeVersion,
  "io.circe"          %% "circe-refined"              % circeVersion,
  "io.chrisdavenport" %% "log4cats-slf4j"             % log4catsVersion

)

//test Dependencies
libraryDependencies ++= Seq(
  "org.scalacheck" %% "scalacheck"         % "1.14.0",
  "eu.timepit"     %% "refined-scalacheck" % "0.9.0"
).map( _ %  "test" )

//Java Dependencies
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.7.7",
  "org.apache.avro" % "avro" % "1.8.2",
  "org.apache.hbase" % "hbase-client" % "2.1.0",
  "org.apache.hbase" % "hbase-common" % "2.1.0",
)

/*
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("fs2.**" -> "fs2-http4s.@1").inLibrary(
    "org.http4s"        %% "http4s-dsl"                 % http4sVersion,
    "org.http4s"        %% "http4s-blaze-client"        % http4sVersion,
    "org.http4s"        %% "http4s-core"               % http4sVersion,
    "org.http4s"        %% "http4s-circe"               % http4sVersion
  ).inProject
)*/
assemblyMergeStrategy in assembly := {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last == "pom.xml" || ps.last == "pom.properties" || ps.last == "io.netty.versions.properties" =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.deduplicate
    }
  case x => MergeStrategy.first
}

mainClass in assembly := Some("com.scigility.exec.KafkaToHbase")

enablePlugins(TutPlugin)

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.6")
