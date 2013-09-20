/**
 * Part of a blog entry at: http://stevenskelton.ca/binary-semaphore-filter/
 */
import com.twitter.finagle.{ Service, SimpleFilter, TransportException }
import com.twitter.util.{ Time, Future }
import scala.util.{ Try, Success, Failure }
import scala.collection.mutable.{ HashMap, SynchronizedMap }
import com.google.common.hash.Hashing
import org.apache.commons.codec.binary.Base64

/**
 * Calls necessary to change the SeqId for a
 *  TBinaryProtocol request.
 */
trait GetAndSetSeqId {
  def get32(buf: Array[Byte], off: Int) =
    ((buf(off + 0) & 0xff) << 24) |
      ((buf(off + 1) & 0xff) << 16) |
      ((buf(off + 2) & 0xff) << 8) |
      (buf(off + 3) & 0xff)

  def put32(buf: Array[Byte], off: Int, x: Int) {
    buf(off) = (x >> 24 & 0xff).toByte
    buf(off + 1) = (x >> 16 & 0xff).toByte
    buf(off + 2) = (x >> 8 & 0xff).toByte
    buf(off + 3) = (x & 0xff).toByte
  }

  def badMsg(why: String) = Failure(new IllegalArgumentException(why))

  def getAndSetId(buf: Array[Byte], newId: Int): Try[Int] = {
    if (buf.size < 4) return badMsg("short header")
    val header = get32(buf, 0)
    val off = if (header < 0)
      4 + 4 + get32(buf, 4)
    else 4 + header + 1

    if (buf.size < off + 4) return badMsg("short buffer")

    val currentId = get32(buf, off)
    put32(buf, off, newId)
    Success(currentId)
  }
}
/**
 * Filter to stop multiple identical queries from executing simultaneously.
 * If more than 1 identical query comes in, it will wait and return the same
 *  result as the first one.
 * After a query responds, any re-query will result in a new `service` call.
 */
class BinarySemaphoreFilter extends SimpleFilter[Array[Byte], Array[Byte]] with GetAndSetSeqId {

  def requestHashKey(request: Array[Byte]): String =
    Base64.encodeBase64String(Hashing.md5.hashBytes(request).asBytes)

  /** Outstanding requests. */
  val inProcess = new HashMap[String, Future[Array[Byte]]] with SynchronizedMap[String, Future[Array[Byte]]]

  def apply(request: Array[Byte], service: Service[Array[Byte], Array[Byte]]): Future[Array[Byte]] = {
    val zeroedSeqId = request.clone
    val seqId = getAndSetId(zeroedSeqId, 0) match {
      case Success(v) => v
      case Failure(e) => return Future.exception(e)
    }
    val key = requestHashKey(zeroedSeqId)
    inProcess.getOrElseUpdate(key, {
      service(zeroedSeqId).ensure({
        inProcess.remove(key)
      })
    }).map(r => {
      val zerodSeqIdResponse = r.clone
      getAndSetId(zerodSeqIdResponse, seqId)
      zerodSeqIdResponse
    })
  }
}