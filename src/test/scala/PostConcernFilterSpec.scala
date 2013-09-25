import org.specs2._
import org.specs2.matcher._
import com.twitter.finagle.builder.{ ServerBuilder, ClientBuilder }
import com.twitter.finagle.thrift._
import ca.stevenskelton.thrift.testservice._
import org.apache.thrift.protocol.TBinaryProtocol
import com.twitter.util.Future

class PostConcernFilterSpec extends mutable.SpecificationWithJUnit {

  sequential

  class PostConcernOfTestService extends TestApi.FutureIface {

    val actions = new scala.collection.mutable.ListBuffer[String]

    def wNoDelay(id: Int): Future[SampleStruct] = {
      id match {
        case 1 | 10 => actions.append(id.toString)
        case 100 => {
          actions.append(id.toString)
          Future.exception(new Exception("Good."))
        }
        case 2 => Future.exception(new Exception("Bad."))
        case 3 => new Exception("Bad.")
        case _ => actions.append("bad, an exception should have been thrown by the original service.")
      }
      Future.never
    }

    def w100msDelay(id: Int): Future[SampleStruct] = Future.never
    def w200msDelay(id: Int): Future[SampleStruct] = Future.never
    def w1sDelay(id: Int): Future[SampleStruct] = Future.never
  }

  val socket = com.twitter.util.RandomSocket()
  val postService = new PostConcernOfTestService
  val filter = new PostConcernFilter(new TestApi$FinagleService(postService, new TBinaryProtocol.Factory))

  val server = ServerBuilder()
    .codec(ThriftServerFramedCodec())
    .name("FooBar")
    .bindTo(socket)
    .build(filter andThen new TestApi$FinagleService(new TestService, new TBinaryProtocol.Factory))

  val clientService = ClientBuilder().codec(ThriftClientFramedCodec()).hosts(socket).hostConnectionLimit(2).build()
  val client = new TestApi$FinagleClient(clientService)

  "make successful calls" in {
    postService.actions.clear

    client.wNoDelay(1).get.name === "Id1"
    postService.actions.toList must containAllOf(Seq("1"))

    client.wNoDelay(10).get.name === "Id10"
    postService.actions.toList must containAllOf(Seq("1", "10")).inOrder

    client.wNoDelay(1).get.name === "Id1"
    postService.actions.toList must containAllOf(Seq("1", "10", "1")).inOrder
  }

  "handle original service exceptions" in {
    postService.actions.clear

    client.wNoDelay(2).get must throwA[NotFoundException]
    postService.actions must beEmpty

    client.wNoDelay(3).get must throwA[DisabledException]
    postService.actions must beEmpty

    client.wNoDelay(4).get must throwA[org.apache.thrift.TApplicationException]
    postService.actions must beEmpty
  }

  "swallow internal exceptions" in {
    postService.actions.clear

    client.wNoDelay(100).get.name === "Id100"
    postService.actions.toList must containAllOf(Seq("100"))
  }
}

