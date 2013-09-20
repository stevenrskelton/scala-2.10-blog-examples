retrieveManaged := true

scalaVersion := "2.10.2"

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/maven-releases"

resolvers += "Twttr" at "http://maven.twttr.com/"

newSettings

libraryDependencies ++= Seq(
	"org.apache.thrift" % "libthrift" % "0.9.1",
	"com.twitter" %% "finagle-thrift" % "6.6.2",
	"com.twitter" %% "finagle-ostrich4" % "6.6.2",
	"com.twitter" %% "scrooge-runtime" % "3.8.0",
	"com.twitter" %% "scrooge-generator" % "3.8.0",
	"org.specs2" %% "specs2" % "2.2.2" % "test",
	"junit" % "junit" % "4.11" % "test"
)