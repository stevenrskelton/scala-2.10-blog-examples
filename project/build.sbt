scalaVersion := "2.10.2"

sbtPlugin := true

name := "scrooge-sbt-plugin"

retrieveManaged := true

resolvers += "Twttr" at "http://maven.twttr.com/"

resolvers += "Maven UK" at "http://uk.maven.org/maven2"

libraryDependencies ++= Seq(
	"com.twitter" %% "scrooge-runtime" % "3.8.0",
	"com.twitter" %% "scrooge-generator" % "3.8.0"
)