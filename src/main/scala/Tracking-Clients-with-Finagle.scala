/**
 * Part of a blog entry at:
 */
import com.twitter.finagle.{ Service, SimpleFilter, TransportException }
import com.twitter.util.{ Time, Future, Try, Return, Throw }
import com.twitter.finagle.thrift.{ SeqIdFilter, ThriftClientRequest, SeqMismatchException }
import scala.util.Random

/** Creates SeqId that modulo to the clientId */
class MultiClientSeqIdFilter(clientId: Int, numberOfClients: Int = 100) extends SeqIdFilter {
  import SeqIdFilter._

  require(numberOfClients > 0, "Number of clients must be greater than zero")
  require(clientId >= 0, "clientId must be >= 0")
  require(clientId < numberOfClients, "clientId must be < numberOfClients")

  val intRange = (2147483647 - numberOfClients) / numberOfClients

  // Why random? Since the underlying codec currently does serial
  // dispatching, it doesn't make any difference, but technically we
  // need to ensure that we pick IDs from a free pool.
  private[this] val rng = new Random(Time.now.inMilliseconds)

  private[this] def get32(buf: Array[Byte], off: Int) =
    ((buf(off + 0) & 0xff) << 24) |
      ((buf(off + 1) & 0xff) << 16) |
      ((buf(off + 2) & 0xff) << 8) |
      (buf(off + 3) & 0xff)

  private[this] def put32(buf: Array[Byte], off: Int, x: Int) {
    buf(off) = (x >> 24 & 0xff).toByte
    buf(off + 1) = (x >> 16 & 0xff).toByte
    buf(off + 2) = (x >> 8 & 0xff).toByte
    buf(off + 3) = (x & 0xff).toByte
  }

  private[this] def badMsg(why: String) = Throw(new IllegalArgumentException(why))

  private[this] def getAndSetId(buf: Array[Byte], newId: Int): Try[Int] = {
    if (buf.size < 4) return badMsg("short header")
    val header = get32(buf, 0)
    val off = if (header < 0) {
      // [4]header
      // [4]n
      // [n]string
      // [4]seqid
      if ((header & VersionMask) != Version1)
        return badMsg("bad version %d".format(header & VersionMask))
      if (buf.size < 8) return badMsg("short name size")
      4 + 4 + get32(buf, 4)
    } else {
      // [4]n
      // [n]name
      // [1]type
      // [4]seqid
      4 + header + 1
    }

    if (buf.size < off + 4) return badMsg("short buffer")

    val currentId = get32(buf, off)
    put32(buf, off, newId)
    Return(currentId)
  }

  override def apply(req: ThriftClientRequest, service: Service[ThriftClientRequest, Array[Byte]]): Future[Array[Byte]] = {
    if (req.oneway) service(req) else {
      val reqBuf = req.message.clone()
      val id = rng.nextInt(intRange) * numberOfClients + clientId
      val givenId = getAndSetId(reqBuf, id) match {
        case Return(id) => id
        case Throw(exc) => return Future.exception(exc)
      }
      val newReq = new ThriftClientRequest(reqBuf, req.oneway)

      service(newReq) flatMap { resBuf =>
        // We know it's safe to mutate the response buffer since the
        // codec never touches it again.
        getAndSetId(resBuf, givenId) match {
          case Return(`id`) => Future.value(resBuf)
          case Return(badId) => Future.exception(SeqMismatchException(badId, id))
          case Throw(exc) => Future.exception(exc)
        }
      }
    }
  }
}

import org.apache.thrift.transport._
import org.apache.thrift.protocol._
import com.twitter.ostrich.stats.Stats

class ApiClientUsageStats(clientIds: Map[Int, String] = Map(), numberOfClients: Int = 100) extends SimpleFilter[Array[Byte], Array[Byte]] {

  /** Record any API method invocations in Ostrich. */
  def apply(request: Array[Byte], service: Service[Array[Byte], Array[Byte]]): Future[Array[Byte]] = {
    val inputTransport = new TMemoryInputTransport(request)
    val binaryProtocol = new TBinaryProtocol(inputTransport)
    val msg = binaryProtocol.readMessageBegin
    val name = msg.name
    val clientId = msg.seqid % numberOfClients
    val client = clientIds.getOrElse(clientId, clientId.toString)

    val now = (new java.util.Date).toString
    Stats.setLabel(s"Last Call: `$name`", now)
    Stats.setLabel(s"Last Call: `$name` by $client: ", now)
    Stats.timeFutureMillis(s"API: `$name`") {
      Stats.timeFutureMillis(s"API: `$name` by $client") {
        service(request).onFailure {
          case rescueException => {
            println(client + ":" + rescueException)
            Future.exception(rescueException)
          }
        }
      }
    }
  }
}