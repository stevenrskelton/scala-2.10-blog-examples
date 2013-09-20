/**
 * Part of a blog entry at:
 */
import com.twitter.finagle.{ Service, SimpleFilter }
import com.twitter.util.Future
import org.apache.thrift.transport._
import org.apache.thrift.protocol._
import com.twitter.scrooge.ThriftStructCodec3

object ProtocolHelpers {

  /**
   * Reads in Thrift data of one protocol, parses it, and outputs in another protocol.
   * There is gc overhead doing this, as all data must be deserialized into class instances.
   */
  def reserialize(
    finagleServiceObject: AnyRef,
    input: Array[Byte],
    outputProtocolFactory: TProtocolFactory = new TSimpleJSONProtocol.Factory,
    inputProtocolFactory: TProtocolFactory = new TBinaryProtocol.Factory): Array[Byte] = {

    import com.twitter.scrooge.ThriftStructCodec3

    val inputTransport = new TMemoryInputTransport(input)
    val inputProtocol = inputProtocolFactory.getProtocol(inputTransport)
    val msg = inputProtocol.readMessageBegin
    import org.apache.thrift.protocol.TMessageType
    val classPostfix = msg.`type` match {
      case TMessageType.CALL => "$args$"
      case TMessageType.REPLY => "$result$"
    }
    val className = finagleServiceObject.getClass.getName + msg.name + classPostfix
    val outputTransport = new TMemoryBuffer(255)
    val outputProtocol = outputProtocolFactory.getProtocol(outputTransport)
    outputProtocol.writeMessageBegin(msg)
    val clazz = java.lang.Class.forName(className)
    val codec = clazz.getField("MODULE$").get(clazz).asInstanceOf[ThriftStructCodec3[_]]
    val args = codec.decode(inputProtocol)
    //can't call encode because of type erasure, call using reflection
    val encodeMethod = clazz.getMethods.filter(_.getName == "encode").head
    encodeMethod.invoke(codec, args.asInstanceOf[Object], outputProtocol)
    outputProtocol.writeMessageEnd
    outputTransport.getArray.slice(0, outputTransport.length)
  }
}

class BinaryProtocolToJsonLoggingFilter(
  finagleServiceObject: AnyRef,
  write: String => Unit,
  toProtocol: TProtocolFactory = new TSimpleJSONProtocol.Factory) extends SimpleFilter[Array[Byte], Array[Byte]] {

  import ProtocolHelpers.reserialize

  def apply(request: Array[Byte], service: Service[Array[Byte], Array[Byte]]): Future[Array[Byte]] = {
    val requestJson = reserialize(finagleServiceObject, request, toProtocol)
    write(new String(requestJson, "UTF-8"))

    service(request).onSuccess(response => {
      val responseJson = reserialize(finagleServiceObject, response, toProtocol)
      write(new String(responseJson, "UTF-8"))
    })
  }
}