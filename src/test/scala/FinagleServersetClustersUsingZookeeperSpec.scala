/**
 * Part of a blog entry at: http://stevenskelton.ca/finagle-serverset-clusters-using-zookeeper/
 */
import org.specs2._
import org.specs2.matcher._
import com.twitter.finagle.builder.{ ServerBuilder, ClientBuilder }
import com.twitter.finagle.thrift._
import org.apache.thrift.protocol.TBinaryProtocol
import ca.stevenskelton.thrift.testservice._
import com.twitter.common.zookeeper.{ ServerSets, ServerSetImpl, ZooKeeperClient }
import com.twitter.finagle.zookeeper.ZookeeperServerSetCluster

class FinagleServersetClustersUsingZookeeperSpec extends mutable.SpecificationWithJUnit {

  sequential
  
  val socket1 = com.twitter.util.RandomSocket()
  val socket2 = com.twitter.util.RandomSocket()
  val testService1 = new TestService
  val testService2 = new TestService

  val server1 = ServerBuilder()
    .codec(ThriftServerFramedCodec())
    .name("server1")
    .bindTo(socket1)
    .build(new TestApi$FinagleService(new TestService, new TBinaryProtocol.Factory))

  val server2 = ServerBuilder()
    .codec(ThriftServerFramedCodec())
    .name("server2")
    .bindTo(socket2)
    .build(new TestApi$FinagleService(new TestService, new TBinaryProtocol.Factory))

  val zk = new com.twitter.finagle.zookeeper.ZkInstance
  zk.start

  "static client works" in {
    val staticService = ClientBuilder()
      .codec(ThriftClientFramedCodec())
      .hosts(Seq(socket1, socket2))
      .hostConnectionLimit(2)
      .build()

    val staticClient = new TestApi$FinagleClient(staticService)
    staticClient.wNoDelay(1).get.id === 1
  }

  "dynamic client works" in {
    val serverSet = new ServerSetImpl(zk.zookeeperClient, "/testservice")
    val cluster = new ZookeeperServerSetCluster(serverSet)

    val dynamicService = ClientBuilder()
      .codec(ThriftClientFramedCodec())
      .cluster(cluster)
      .hostConnectionLimit(2)
      .build()

    //static servers join the cluster
    cluster.join(server1.localAddress)
    cluster.join(server2.localAddress)

    val dynamicClient = new TestApi$FinagleClient(dynamicService)
    dynamicClient.wNoDelay(1).get.id === 1
  }

  "deserialize ServiceInstance" in {
    val serverSet = new ServerSetImpl(zk.zookeeperClient, "/testservice")
    val cluster = new ZookeeperServerSetCluster(serverSet)

    cluster.join(socket1)

    val jsonCodec = ServerSetImpl.createJsonCodec
    val zkClient = zk.zookeeperClient.get

    import scala.collection.JavaConversions._
    val serverInstances = for (zNode <- zkClient.getChildren("/testservice", false)) yield {
      val serverData = zkClient.getData("/testservice/" + zNode, false, null)
      ServerSets.deserializeServiceInstance(serverData, jsonCodec)
    }
    serverInstances.filter(s => {
      s.serviceEndpoint.host == socket1.getHostName &&
        s.serviceEndpoint.port == socket1.getPort
    }) must not beEmpty
  }
}