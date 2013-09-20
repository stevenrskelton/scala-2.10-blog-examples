import org.specs2._
import org.specs2.matcher._
import com.twitter.finagle.builder.{ ServerBuilder, ClientBuilder }
import com.twitter.finagle.thrift._
import org.apache.thrift.protocol.TBinaryProtocol
import ca.stevenskelton.thrift.testservice._
import MultiplexedFinagleService.multiplexedBinaryProtocolFactory

class MultiplexedFinagleServiceSpec extends mutable.SpecificationWithJUnit {

  val socket = com.twitter.util.RandomSocket()
  val serviceMap = Map(
    "FooApi" -> new TestApi.FinagledService(new TestService,
      multiplexedBinaryProtocolFactory("FooApi")),

    "BarApi" -> new TestApi.FinagledService(new TestService,
      multiplexedBinaryProtocolFactory("BarApi")))

  val server = ServerBuilder()
    .codec(ThriftServerFramedCodec())
    .name("FooBar")
    .bindTo(socket)
    .build(new MultiplexedFinagleService(serviceMap))

  val service = ClientBuilder()
    .codec(ThriftClientFramedCodec())
    .hosts(socket)
    .hostConnectionLimit(1)
    .build()

  val fooClient = new TestApi.FinagledClient(service,
    multiplexedBinaryProtocolFactory("FooApi"))

  val barClient = new TestApi.FinagledClient(service,
    multiplexedBinaryProtocolFactory("BarApi"))

  "return success" in {
    fooClient.wNoDelay(1).get.name === "Id1"
    barClient.wNoDelay(10).get.name === "Id10"
  }
}