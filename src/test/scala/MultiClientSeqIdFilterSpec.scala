import org.specs2._
import org.specs2.matcher._
import com.twitter.finagle.builder.{ ServerBuilder, ClientBuilder }
import com.twitter.finagle.thrift._
import ca.stevenskelton.thrift.testservice._
import org.apache.thrift.protocol.TBinaryProtocol

class MultiClientSeqIdFilterSpec extends mutable.SpecificationWithJUnit {

  sequential

  val socket = com.twitter.util.RandomSocket()
  val numberOfClients = 4
  val thisApplicationsClientId = 3
  val filter = new MultiClientSeqIdFilter(0, numberOfClients)

  val codecFactory = new ThriftClientFramedCodecFactory(None, true, new TBinaryProtocol.Factory())

  val clientService = ClientBuilder().codec(codecFactory).hosts(socket).hostConnectionLimit(2).build()
  val client = new TestApi$FinagleClient(filter andThen clientService)
  val client1 = new TestApi$FinagleClient(new MultiClientSeqIdFilter(1, numberOfClients) andThen clientService)
  val client2 = new TestApi$FinagleClient(new MultiClientSeqIdFilter(2, numberOfClients) andThen clientService)

  val consoleLogFilter = new BinaryProtocolToJsonLoggingFilter(TestApi, println)
  val apiUsageFilter = new ApiClientUsageStats(Map(1 -> "ClientA", 2 -> "ClientB", 3 -> "ClientC"), numberOfClients)

  val service = ServerBuilder().codec(ThriftServerFramedCodec()).bindTo(socket).name("test")
    .build(apiUsageFilter andThen consoleLogFilter andThen new TestApi$FinagleService(new TestService, new TBinaryProtocol.Factory))

  "test from log output" in {
    client.wNoDelay(1).get
    client.wNoDelay(1).get
    client.wNoDelay(1).get
    client.wNoDelay(1).get

    Seq(358298968, 496764616, 815307780, 265888436).forall(_ % 4 === 0)
    Seq(419392489, 479979617, 1836519265, 1751983461).forall(_ % 4 === 1)
    Seq(335795530, 1407291194, 1993271466, 1194343190).forall(_ % 4 === 2)
    Seq(1579114335, 1541072923, 898646695, 706294983).forall(_ % 4 === 3)

    client1.wNoDelay(1).get
    client2.wNoDelay(1).get
    client.wNoDelay(1).get
    client1.wNoDelay(1).get
    client1.wNoDelay(1).get
    client2.wNoDelay(1).get
    client2.wNoDelay(1).get
    client2.wNoDelay(1).get

    import com.twitter.ostrich.stats.Stats
    println("Labels:")
    Stats.getLabels.foreach {
      case (label, value) => {
        println(label + "     " + value)
      }
    }
    Stats.getMetrics.foreach {
      case (label, value) => {
        println(label + "     " + value)
      }
    }

    success
  }

  "shards" in {
    import scala.util.Random
    import com.twitter.util.Time

    val intRange = (2147483647 - numberOfClients) / numberOfClients
    val rng = new Random(Time.now.inMilliseconds)
    for (i <- 0 to 5000) {
      val id = rng.nextInt(intRange) * numberOfClients
      for (shardId <- 0 until numberOfClients) {
        val idShard = id + shardId
        (idShard % numberOfClients) === shardId
        for (otherShardId <- 0 until numberOfClients if otherShardId != shardId) {
          (idShard % numberOfClients) !== otherShardId
        }
      }
    }
    success
  }

  "exceptions" in {
    client.wNoDelay(1).get === SampleStruct(1, "Id1")
    client.wNoDelay(2).get must throwA[NotFoundException]
    client.wNoDelay(3).get must throwA[DisabledException]
    client.wNoDelay(4).get must throwA[org.apache.thrift.TApplicationException]

  }
}

