import org.specs2._
import org.specs2.matcher._
import com.twitter.finagle.builder.{ ServerBuilder, ClientBuilder }
import com.twitter.finagle.thrift._
import org.apache.thrift.protocol.TBinaryProtocol
import ca.stevenskelton.thrift.testservice._
import MultiplexedFinagleService.multiplexedBinaryProtocolFactory

class MultiplexedFinagleServiceSpec extends mutable.SpecificationWithJUnit {

  val socket = com.twitter.util.RandomSocket()
  val fooService = new TestService
  val barService = new TestService
  val serviceMap = Map(
    "FooApi" -> new TestApi$FinagleService(fooService,
      multiplexedBinaryProtocolFactory("FooApi")),

    "BarApi" -> new TestApi$FinagleService(barService,
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

  val fooClient = new TestApi$FinagleClient(service,
    multiplexedBinaryProtocolFactory("FooApi"))

  val barClient = new TestApi$FinagleClient(service,
    multiplexedBinaryProtocolFactory("BarApi"))

  "return success" in {
    fooClient.wNoDelay(1).get.name === "Id1"
    barClient.wNoDelay(10).get.name === "Id10"
    fooService.wNoDelayCount.get === 1
    barService.wNoDelayCount.get === 1
  }
}