import org.specs2._
import org.specs2.matcher._
import com.twitter.finagle.builder.{ ServerBuilder, ClientBuilder }
import com.twitter.finagle.thrift._
import org.apache.thrift.protocol._
import ca.stevenskelton.thrift.testservice._

class BinaryProtocolToJsonLoggingSpec extends mutable.SpecificationWithJUnit {

  sequential

  val socket = com.twitter.util.RandomSocket()

  val service = ClientBuilder()
    .codec(ThriftClientFramedCodec())
    .hosts(socket)
    .hostConnectionLimit(1)
    .build()

  val fooClient = new TestApi.FinagledClient(service, new TBinaryProtocol.Factory)

  "SimpleJSON call" in {
    val consoleLogFilter = new BinaryProtocolToJsonLoggingFilter(TestApi, println, new TSimpleJSONProtocol.Factory)
    val server = ServerBuilder()
      .codec(ThriftServerFramedCodec())
      .name("FooBar")
      .bindTo(socket)
      .build(consoleLogFilter andThen new TestApi$FinagleService(new TestService, new TBinaryProtocol.Factory))

    fooClient.wNoDelay(1).get.name === "Id1"
    server.close().get() === ()
  }

  "JSON call" in {
    val consoleLogFilter = new BinaryProtocolToJsonLoggingFilter(TestApi, println, new TJSONProtocol.Factory)
    val server = ServerBuilder()
      .codec(ThriftServerFramedCodec())
      .name("FooBar")
      .bindTo(socket)
      .build(consoleLogFilter andThen new TestApi$FinagleService(new TestService, new TBinaryProtocol.Factory))
    fooClient.wNoDelay(1).get.name === "Id1"
    server.close().get() === ()

  }
}