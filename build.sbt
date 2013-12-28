retrieveManaged := true

scalaVersion := "2.10.3"

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/maven-releases"

resolvers += "Twttr" at "http://maven.twttr.com/"

newSettings

libraryDependencies ++= Seq(
	"org.apache.thrift" % "libthrift" % "0.9.1",
	"com.twitter" %% "finagle-thrift" % "6.10.0",
	"com.twitter" %% "finagle-serversets" % "6.10.0",
	"com.twitter" %% "finagle-ostrich4" % "6.10.0",
	"com.twitter" %% "scrooge-runtime" % "3.11.2",
	"com.twitter" %% "scrooge-generator" % "3.11.2",
	"org.specs2" %% "specs2" % "2.3.7" % "test",
	"junit" % "junit" % "4.11" % "test"
)