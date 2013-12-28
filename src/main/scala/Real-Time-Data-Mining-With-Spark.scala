
import scala.xml.{ NodeSeq, MetaData }
import java.io.File
import scala.io.{ BufferedSource, Source }

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.log4j.{ LogManager, Level }

object StackOverflowMain extends App {
  //LogManager.getRootLogger().setLevel(Level.WARN)

  val sc = new SparkContext("local", "Main")
  val minSplits = 1
  val jsonData = sc.textFile(Post.file.getAbsolutePath, minSplits)
  val objData = jsonData.flatMap(Post.parse)
  objData.cache
  
  var query: RDD[Post] = objData

  println("Enter new command:")
  do {
  } while (readCommand)
  println("Exit")

  def readCommand: Boolean = {
    val command = readLine
    if (command.isEmpty) false
    else {
      //...match commands
      command match {
        case c if c.startsWith("t:") => {
          //filter for posts that contain any of the comma separated list of tags.
          val tags = c.drop(2).split(",").toSet
          query = query.filter(_.tags.exists(tags.contains))
        }
        case c if c.startsWith("d:") => {
          //filter for posts that are within the date range
          val d = c.drop(2).split(",").map(i => Post.dateFormat.parse(i + "T00:00:00.000").getTime)
          query = query.filter(n => n.creationDate >= d(0) && n.creationDate < d(1))
        }
        case "!" => time("Count") {
          println(query.count)
        }
        case "!t" => time("Tags") {
          val tags = query.flatMap(_.tags).countByValue
          println(tags.toSeq.sortBy(_._2 * -1).take(10).mkString(","))
        }
        case "~" => {
          //reset all filters applied to query
          query = objData
        }
      }
      true
    }
  }
  def time[T](name: String)(block: => T): T = {
    val startTime = System.currentTimeMillis
    val result = block // call-by-name
    println(s"$name: ${System.currentTimeMillis - startTime}ms")
    result
  }
}

abstract class StackTable[T] {

  val file: File

  def getDate(n: scala.xml.NodeSeq): Long = n.text match {
    case "" => 0
    case s => dateFormat.parse(s).getTime
  }

  def dateFormat = {
    import java.text.SimpleDateFormat
    import java.util.TimeZone
    val f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    f.setTimeZone(TimeZone.getTimeZone("GMT"))
    f
  }

  def getInt(n: scala.xml.NodeSeq): Int = n.text match {
    case "" => 0
    case x => x.toInt
  }

  def parseXml(x: scala.xml.Elem): T

  def parse(s: String): Option[T] =
    if (s.startsWith("  <row ")) Some(parseXml(scala.xml.XML.loadString(s)))
    else None

}

object Post extends StackTable[Post] {

  val file = new File("data/Posts.xml")
  assert(file.exists)

  override def parseXml(x: scala.xml.Elem): Post = Post(
    getInt(x \ "@Id"),
    getInt(x \ "@PostTypeId"),
    getInt(x \ "@AcceptedAnswerId"),
    getDate(x \ "@CreationDate"),
    getInt(x \ "@Score"),
    getInt(x \ "@ViewCount"),
    (x \ "@Body").text,
    getInt(x \ "@OwnerUserId"),
    getDate(x \ "@LastActivityDate"),
    (x \ "@Title").text,
    getTags(x \ "@Tags"),
    getInt(x \ "@AnswerCount"),
    getInt(x \ "@CommentCount"),
    getInt(x \ "@FavoriteCount"),
    getDate(x \ "@CommunityOwnedDate"))

  def getTags(x: scala.xml.NodeSeq): Array[String] = x.text match {
    case "" => Array()
    case s => s.drop(1).dropRight(1).split("><")
  }
}

// <row Id="1" PostTypeId="1" AcceptedAnswerId="15" CreationDate="2010-07-19T19:12:12.510" Score="19" ViewCount="1033" Body="&lt;p&gt;How should I elicit prior distributions from experts when fitting a Bayesian model?&lt;/p&gt;&#xA;" OwnerUserId="8" LastActivityDate="2010-09-15T21:08:26.077" Title="Eliciting priors from experts" Tags="&lt;bayesian&gt;&lt;prior&gt;&lt;elicitation&gt;" AnswerCount="5" CommentCount="1" FavoriteCount="11" />
case class Post(
  id: Int,
  postTypeId: Int,
  acceptedAnswerId: Int,
  creationDate: Long,
  score: Int,
  viewCount: Int,
  body: String,
  ownerUserId: Int,
  lastActivityDate: Long,
  title: String,
  tags: Array[String],
  answerCount: Int,
  commentCount: Int,
  favoriteCount: Int,
  communityOwnedDate: Long)