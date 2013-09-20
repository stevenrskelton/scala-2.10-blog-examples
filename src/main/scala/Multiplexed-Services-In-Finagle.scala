/**
 * Part of a blog entry at:
 */
import com.twitter.util.Future
import org.apache.thrift.transport.{ TTransport, TMemoryInputTransport }
import org.apache.thrift.protocol._
import org.apache.thrift.TException
import com.twitter.finagle.Service

/**
 * Extension of standard Finagle Service; multiplexes sub-services specified in serviceMap.
 * Mimics the behaviour of a TMultiplexedProcessor.
 */
class MultiplexedFinagleService(val serviceMap: Map[String, Service[Array[Byte], Array[Byte]]])
  extends Service[Array[Byte], Array[Byte]] {
  import MultiplexedFinagleService._

  val protocolFactory = new TBinaryProtocol.Factory

  final def apply(request: Array[Byte]): Future[Array[Byte]] = {
    val inputTransport = new TMemoryInputTransport(request)
    val iprot = protocolFactory.getProtocol(inputTransport)
    try {
      val msg = iprot.readMessageBegin
      val (serviceName, methodName) = splitServiceNameFromMethod(msg)
      val service = serviceMap.getOrElse(serviceName, throw new TException(s"Service `$serviceName` not found."))
      val mappedRequest = mapBinaryProtocolRequestToNewMethod(request, msg.name, methodName)
      service.apply(mappedRequest)
    } catch {
      case e: Exception => Future.exception(e)
    }
  }

  /**
   *   Splits the multiplexed method into service and method name
   */
  private[this] def splitServiceNameFromMethod(message: TMessage): (String, String) = {
    /** Used to delimit the service name from the function name */

    // Extract the service name
    val index = message.name.indexOf(TMultiplexedProtocol.SEPARATOR)
    if (index < 0) {
      throw new TException("Service name not found in message name: "
        + message.name + ".  Did you "
        + "forget to use a TMultiplexProtocol in your client?")
    }
    (message.name.substring(0, index), message.name.substring(index + 1))
  }

  /**
   *  Modifies a BinaryProtocol request, mapping original method name to another.
   */
  private[this] def mapBinaryProtocolRequestToNewMethod(request: Array[Byte], originalMethodName: String, newMethodName: String): Array[Byte] = {
    val versionLength = 4 //first byte
    val stringLength = 4 //second byte
    val version = request.take(versionLength)
    val body = request.seq.drop(versionLength + stringLength + originalMethodName.getBytes.size)

    val newMethodNameBytes = newMethodName.getBytes("UTF-8")
    val newStringLength = int32ToBytes(newMethodNameBytes.size)

    val response = new Array[Byte](versionLength + stringLength + newMethodNameBytes.size + body.size)
    version.copyToArray(response, 0)
    newStringLength.copyToArray(response, versionLength)
    newMethodNameBytes.copyToArray(response, versionLength + stringLength)
    body.copyToArray(response, versionLength + stringLength + newMethodNameBytes.size)
    response
  }

  /**
   * Get 4 bytes for Int32
   */
  private[this] def int32ToBytes(i32: Int): Array[Byte] = {
    Array(
      (0xff & (i32 >> 24)).toByte,
      (0xff & (i32 >> 16)).toByte,
      (0xff & (i32 >> 8)).toByte,
      (0xff & (i32)).toByte)
  }
}
/**
 * Code necessary to implement a multiplexed Finagle service.
 */
object MultiplexedFinagleService {
  /**
   * Create a protocol factory that will wrap the standard Finagle BinaryProtocol within
   *  a MultiplexedProtocol wrapper.
   */
  def multiplexedBinaryProtocolFactory(serviceName: String): org.apache.thrift.protocol.TProtocolFactory = {
    new {} with TBinaryProtocol.Factory {
      override def getProtocol(trans: TTransport): TProtocol = {
        new TMultiplexedProtocol(super.getProtocol(trans), serviceName)
      }
    }
  }
}