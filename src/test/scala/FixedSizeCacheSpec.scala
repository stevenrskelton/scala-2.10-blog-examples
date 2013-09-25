import org.specs2._
import org.specs2.matcher._
import com.twitter.finagle.builder.{ ServerBuilder, ClientBuilder }
import com.twitter.finagle.thrift._
import ca.stevenskelton.thrift.testservice._
import org.apache.thrift.protocol.TBinaryProtocol

class FixedSizeCacheSpec extends mutable.SpecificationWithJUnit {

  sequential

  val socket = com.twitter.util.RandomSocket()
  val clientService = ClientBuilder().codec(ThriftClientFramedCodec()).hosts(socket).hostConnectionLimit(2).build()
  val client = new TestApi$FinagleClient(clientService)

  val filter = new BinaryProtocolToJsonLoggingFilter(TestApi, println) andThen new FixedSizeCache(Seq("w1sDelay", "w200msDelay"))

  val service = ServerBuilder().codec(ThriftServerFramedCodec()).bindTo(socket).name("test")
    .build(filter andThen new TestApi$FinagleService(new TestService, new TBinaryProtocol.Factory))

  "not cache methods not in list" in {

    var start = System.currentTimeMillis
    client.w100msDelay(1).get.name === "Id1"
    System.currentTimeMillis - start must beGreaterThanOrEqualTo(100L)

    start = System.currentTimeMillis
    client.w100msDelay(1).get.name === "Id1"
    System.currentTimeMillis - start must beGreaterThanOrEqualTo(100L)

  }

  "cache methods in list" in {
    var start = System.currentTimeMillis
    client.w200msDelay(1).get.name === "Id1"
    System.currentTimeMillis - start must beGreaterThanOrEqualTo(200L)

    start = System.currentTimeMillis
    client.w200msDelay(1).get.name === "Id1"
    System.currentTimeMillis - start must beLessThan(200L)
  }

  "mixed" in {
    var start = System.currentTimeMillis
    client.w200msDelay(1).get.name === "Id1"

    start = System.currentTimeMillis
    client.w200msDelay(10).get.name === "Id10"
    System.currentTimeMillis - start must beGreaterThanOrEqualTo(200L)

    start = System.currentTimeMillis
    client.w200msDelay(1).get.name === "Id1"
    System.currentTimeMillis - start must beLessThan(200L)

    start = System.currentTimeMillis
    client.w200msDelay(100).get.name === "Id100"
    System.currentTimeMillis - start must beGreaterThanOrEqualTo(200L)
  }

  "do not cache exceptions" in {
    client.w200msDelay(1).get.name == "Id1"
    client.w200msDelay(2).get must throwA[NotFoundException]
    client.w200msDelay(3).get must throwA[DisabledException]

    var start = System.currentTimeMillis
    client.w200msDelay(1).get.name == "Id1"
    System.currentTimeMillis - start must beLessThan(175L)

    start = System.currentTimeMillis
    client.w200msDelay(2).get must throwA[NotFoundException]
    System.currentTimeMillis - start must beGreaterThanOrEqualTo(200L)

    start = System.currentTimeMillis
    client.w200msDelay(3).get must throwA[DisabledException]
    System.currentTimeMillis - start must beGreaterThanOrEqualTo(200L)
  }

  "do not cache unhandled exceptions" in {
    var start = System.currentTimeMillis
    client.w200msDelay(4).get must throwA[Exception]
    System.currentTimeMillis - start must beGreaterThanOrEqualTo(200L)

    start = System.currentTimeMillis
    client.w200msDelay(4).get must throwA[Exception]
    System.currentTimeMillis - start must beGreaterThanOrEqualTo(200L)
  }
}

