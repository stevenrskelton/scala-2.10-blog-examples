import org.specs2._
import org.specs2.matcher._
import com.twitter.finagle.builder.{ ServerBuilder, ClientBuilder }
import com.twitter.finagle.thrift._
import java.net.InetSocketAddress
import ca.stevenskelton.thrift.testservice._
import org.apache.thrift.protocol.TBinaryProtocol
import java.util.concurrent.TimeUnit.SECONDS
import com.twitter.util.Duration

class ReusingFinagleServerFiltersOnClientSpec extends mutable.SpecificationWithJUnit {

  class Writer(var value: Option[String] = None) {
    def write(in: String): Unit = value = Some(in)
  }

  val socket = com.twitter.util.RandomSocket()

  val writer = new Writer
  val filter = new MethodNameFilter(writer.write)
  val server = ServerBuilder()
    .codec(ThriftServerFramedCodec())
    .name("FooBar")
    .bindTo(socket)
    .build(filter andThen new TestApi$FinagleService(new TestService, new TBinaryProtocol.Factory))

  val clientWriter = new Writer
  val clientFilter = new MethodNameClientFilter(clientWriter.write)
  val clientService = ClientBuilder().codec(ThriftClientFramedCodec()).hosts(socket).hostConnectionLimit(2).build()
  val client = new TestApi$FinagleClient(clientFilter andThen clientService)

  "filters write method name" in {
    writer.value === None
    clientWriter.value === None

    client.wNoDelay(1).get

    writer.value === Some("wNoDelay")
    clientWriter.value === Some("wNoDelay")
  }
}