scalaVersion := "2.10.2"

sbtPlugin := true

name := "scrooge-sbt-plugin"

resolvers += "Twttr" at "http://maven.twttr.com/"

libraryDependencies ++= Seq(
	"com.twitter" %% "scrooge-runtime" % "3.8.0",
	"com.twitter" %% "scrooge-generator" % "3.8.0"
)

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.3.0")