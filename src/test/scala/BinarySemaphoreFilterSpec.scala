import org.specs2._
import org.specs2.matcher._
import com.twitter.finagle.builder.{ ServerBuilder, ClientBuilder }
import com.twitter.finagle.thrift._
import ca.stevenskelton.thrift.testservice._
import org.apache.thrift.protocol.TBinaryProtocol
import java.util.concurrent.TimeUnit.SECONDS
import com.twitter.util.{ Duration, Future }
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

class BinarySemaphoreFilterSpec extends mutable.SpecificationWithJUnit {

  sequential

  val socket = com.twitter.util.RandomSocket()
  val clientService = ClientBuilder().codec(ThriftClientFramedCodec()).hosts(socket).hostConnectionLimit(10).build()
  val client = new TestApi.FinagledClient(clientService)

  val filter = new BinaryProtocolToJsonLoggingFilter(TestApi, println) andThen new BinarySemaphoreFilter

  "queue items" in {
    val serviceWithoutFilter = future {
      ServerBuilder().codec(ThriftServerFramedCodec()).bindTo(socket).name("test")
        .build(new TestApi.FinagledService(new TestService, new TBinaryProtocol.Factory))
    }

    //start service in different thread
    //wait for service initialization
    //make a call
    //wait until it is almost returned
    //make another call

    Thread.sleep(100)
    client.w200msDelay(1)
    Thread.sleep(175)
    time(client.w200msDelay(1).get) must beGreaterThanOrEqualTo(200L)

    Await.result(serviceWithoutFilter, scala.concurrent.duration.Duration(1, scala.concurrent.duration.SECONDS)).close()

    val service = future {
      ServerBuilder().codec(ThriftServerFramedCodec()).bindTo(socket).name("test")
        .build(filter andThen new TestApi.FinagledService(new TestService, new TBinaryProtocol.Factory))
    }

    Thread.sleep(100)
    client.w200msDelay(1)
    Thread.sleep(175)
    time(client.w200msDelay(1).get) must beLessThan(200L)

    Await.result(service, scala.concurrent.duration.Duration(1, scala.concurrent.duration.SECONDS)).close()

    success
  }

  "release items" in {

    val service = future {
      ServerBuilder().codec(ThriftServerFramedCodec()).bindTo(socket).name("test")
        .build(filter andThen new TestApi.FinagledService(new TestService, new TBinaryProtocol.Factory))
    }

    Thread.sleep(100)
    client.w200msDelay(1)
    Thread.sleep(175)
    time(client.w200msDelay(1).get) must beLessThan(200L)
    Thread.sleep(200)
    time(client.w200msDelay(1).get) must beGreaterThanOrEqualTo(200L)

    Await.result(service, scala.concurrent.duration.Duration(1, scala.concurrent.duration.SECONDS)).close()

    success

  }

  def time[A](a: => A): Long = {
    val now = System.nanoTime
    val result = a
    (System.nanoTime - now) / 1000 / 1000
  }
}