/**
 * Part of a blog entry at: http://stevenskelton.ca/thrift-client-side-caching-to-speed-up-unit-tests/
 */
import com.twitter.finagle.{ Service, SimpleFilter }
import com.twitter.util.Future
import org.apache.thrift.transport._
import org.apache.thrift.protocol._
import org.apache.thrift.protocol.{ TBinaryProtocol, TJSONProtocol }
import com.twitter.finagle.thrift.ThriftClientRequest
import ProtocolHelpers.reserialize
import scala.Array.canBuildFrom
import ProtocolHelpers._

/**
 * 	Returns response from supplied TJSONProtocol log file,
 *   matching on serialization of request (excluding SeqId).
 */
class MockJSONDataFilter(
  finagleServiceObject: AnyRef,
  jsonLog: Seq[(String, String)],
  thriftEncoding: TProtocolFactory = new TBinaryProtocol.Factory)
  extends SimpleFilter[ThriftClientRequest, Array[Byte]] {

  val logEncoding = new TJSONProtocol.Factory

  /**
   * All request/response pairs
   */
  lazy val requestResponses: Map[String, Array[Byte]] = jsonLog.map {
    case (request, response) => {
      val request0 = changeSeqId(request.getBytes("UTF-8"), logEncoding)._2
      val reserializedResponse = reserialize(finagleServiceObject, response.getBytes("UTF-8"), thriftEncoding, logEncoding)
      (new String(request0, "UTF-8"), reserializedResponse)
    }
  }.toMap

  /**
   *  Change SeqId within any TProtocol
   */
  def changeSeqId(requestOrResponse: Array[Byte], protocolFactory: TProtocolFactory, seqId: Int = 0): (Int, Array[Byte]) = {

    val inputTransport = new TMemoryInputTransport(requestOrResponse)
    val inputProtocol = protocolFactory.getProtocol(inputTransport)
    //pull out the TMessage header
    val inputMessage = inputProtocol.readMessageBegin
    //find all data past the header		
    val remainingBytes = inputTransport.getBytesRemainingInBuffer
    val remainingInputMessage = requestOrResponse.slice(requestOrResponse.length - remainingBytes, requestOrResponse.length)

    val outputTransport = new TMemoryBuffer(requestOrResponse.length)
    val outputProtocol = protocolFactory.getProtocol(outputTransport)
    val message0 = new TMessage(inputMessage.name, inputMessage.`type`, seqId)
    outputProtocol.writeMessageBegin(message0)
    //replacement TMessage with our SeqId
    val requestOrResponse0 = outputTransport.getArray.slice(0, outputTransport.length)

    //json protocols expect the next strut to write commas,
    // we need first struct to add it.  Try writing a new empty
    // struct and see if anything is added.
    outputProtocol.writeStructBegin(null)
    val jsonCommaFix = if (outputTransport.length > requestOrResponse0.length)
      //this is a complete hack, we only want the first byte added
      requestOrResponse.slice(requestOrResponse.length - remainingBytes - 1, requestOrResponse.length - remainingBytes)
    else Array[Byte]()
    (inputMessage.seqid, requestOrResponse0 ++ jsonCommaFix ++ remainingInputMessage)
  }

  def apply(request: ThriftClientRequest, service: Service[ThriftClientRequest, Array[Byte]]): Future[Array[Byte]] = {

    val (oldSeqId, request0) = changeSeqId(request.message, thriftEncoding)
    val requestKeyJson = new String(reserialize(finagleServiceObject, request0, logEncoding, thriftEncoding), "UTF-8")

    //try and match request
    val response = requestResponses.getOrElse(requestKeyJson, {
      return Future.exception(new Exception(s"Request signature not found in mock data: $requestKeyJson"))
    })
    Future.value(changeSeqId(response, thriftEncoding, oldSeqId)._2)
  }
}