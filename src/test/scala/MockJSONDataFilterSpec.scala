import org.specs2._
import org.specs2.matcher._
import com.twitter.finagle.builder.{ ServerBuilder, ClientBuilder }
import com.twitter.finagle.thrift._
import ca.stevenskelton.thrift.testservice._
import org.apache.thrift.protocol.TBinaryProtocol

class MockJSONDataFilterSpec extends mutable.SpecificationWithJUnit {

  sequential

  val socket = com.twitter.util.RandomSocket()
  val log = Seq(("""[1,"wNoDelay",1,0,{"1":{"i32":999}}]""", """[1,"wNoDelay",2,0,{"0":{"rec":{"1":{"i32":999},"2":{"str":"Id999"}}}}]"""))

  val filter = new MockJSONDataFilter(TestApi, log)

  val clientService = ClientBuilder().codec(ThriftClientFramedCodec()).hosts(socket).hostConnectionLimit(2).build()
  val client = new TestApi$FinagleClient(filter andThen clientService)

  "read mock data" in {
    client.wNoDelay(999).get.name === "Id999"
  }

  "fail for missing mock data" in {
    client.wNoDelay(1).get must throwA[Exception]
  }
}

