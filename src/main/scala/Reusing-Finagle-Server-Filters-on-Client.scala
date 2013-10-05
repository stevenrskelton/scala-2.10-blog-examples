/**
 * Part of a blog entry at: http://stevenskelton.ca/reusing-finagle-server-filters-on-client/
 */
import com.twitter.finagle.{ Service, SimpleFilter }
import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.util.Future
import org.apache.thrift.transport._
import org.apache.thrift.protocol._

class AbstractMethodNameFilter[Req](action: String => Unit, requestToByte: Req => Array[Byte])
  extends SimpleFilter[Req, Array[Byte]] {

  def apply(request: Req, service: Service[Req, Array[Byte]]): Future[Array[Byte]] = {
    val binaryRequest = requestToByte(request)
    val inputTransport = new TMemoryInputTransport(binaryRequest)
    val iprot = new TBinaryProtocol(inputTransport)
    val msg = iprot.readMessageBegin
    action(msg.name)
    service(request)
  }
}

/**
 * Performs `action` using request's invoked method name.
 */
class MethodNameFilter(action: String => Unit)
  extends AbstractMethodNameFilter[Array[Byte]](action, x => x)

/**
 * Performs `action` using request's invoked method name.
 */
class MethodNameClientFilter(action: String => Unit)
  extends AbstractMethodNameFilter[ThriftClientRequest](action, x => x.message)