/**
 * Part of a blog entry at: http://stevenskelton.ca/finagle-query-cache-with-guava/
 */
import com.twitter.finagle.{ Service, SimpleFilter }
import com.twitter.util.Future
import com.google.common.cache.{ Cache, CacheBuilder }
import com.twitter.conversions.time._
import com.twitter.util.Duration
import com.google.common.cache.Weigher
import com.google.common.hash.Hashing
import java.util.concurrent.TimeUnit.SECONDS
import org.apache.commons.codec.binary.Base64

abstract class AbstractCacheFilter(val methodsToCache: Option[Seq[String]] = None) extends SimpleFilter[Array[Byte], Array[Byte]] {

  import com.google.common.hash.Hashing
  import org.apache.commons.codec.binary.Base64

  val cache: Cache[String, Array[Byte]]

  /** Hash of request to use as key in cache. */
  def requestHashKey(request: Array[Byte]): String =
    Base64.encodeBase64String(Hashing.md5.hashBytes(request).asBytes)

  def bytesToInt(bytes: Array[Byte]): Int = java.nio.ByteBuffer.wrap(bytes).getInt

  def binaryProtocolMethodNameSeqId(request: Array[Byte]): (String, Array[Byte]) = {
    val methodNameLength = bytesToInt(request.slice(4, 8))
    val methodName = new String(request.slice(8, 8 + methodNameLength), "UTF-8")
    val seqId = request.slice(8 + methodNameLength, 12 + methodNameLength)
    (methodName, seqId)
  }

  def binaryProtocolChangeSeqId(requestOrResponse: Array[Byte], seqId: Array[Byte] = Array(0, 0, 0, 0)): Array[Byte] = {
    val methodNameLength = bytesToInt(requestOrResponse.slice(4, 8))
    val retVal = new Array[Byte](requestOrResponse.length)
    requestOrResponse.copyToArray(retVal)
    seqId.copyToArray(retVal, 8 + methodNameLength, 4)
    retVal
  }

  /** Non-zero status is a thrown error */
  def binaryResponseExitStatus(response: Array[Byte]): Byte = {
    val methodNameLength = bytesToInt(response.slice(4, 8))
    //skip over: version,methodLength,method,seqId,msgType,successStruct
    val positionMessageType = 4 + 4 + methodNameLength + 4 + 1 + 1
    response(positionMessageType)
  }

  def apply(request: Array[Byte], service: Service[Array[Byte], Array[Byte]]): Future[Array[Byte]] = {
    val (methodName, seqId) = binaryProtocolMethodNameSeqId(request)
    //cache all, or those specified
    if (!methodsToCache.isDefined || methodsToCache.get.contains(methodName)) {
      //key using a SeqId == 0
      val key = requestHashKey(binaryProtocolChangeSeqId(request))
      val cachedResult = cache.getIfPresent(key)
      if (cachedResult == null) {
        service(request).onSuccess(r => {
          //store with SeqId == 0, if clean exit
          if (binaryResponseExitStatus(r) == 0) cache.put(key, binaryProtocolChangeSeqId(r))
        })
      } else {
        //change the SeqId to match request
        Future.value(binaryProtocolChangeSeqId(cachedResult, seqId))
      }
    } else service(request)
  }
}

class ExpiryCache(val slidingExpiry: Duration = 60 seconds, val maxExpiry: Duration = 10 minutes) extends AbstractCacheFilter(None) {

  import java.util.concurrent.TimeUnit.SECONDS
  val cache = CacheBuilder.newBuilder()
    .expireAfterAccess(slidingExpiry.inUnit(SECONDS), SECONDS)
    .expireAfterWrite(maxExpiry.inUnit(SECONDS), SECONDS)
    .build[String, Array[Byte]]()
}

class FixedSizeCache(methodsToCache: Seq[String], val maxSizeMegabytes: Int = 100) extends AbstractCacheFilter(Some(methodsToCache)) {

  import com.google.common.cache.Weigher
  val weight = maxSizeMegabytes * 1048576
  val weigher = new Weigher[String, Array[Byte]]() {
    def weigh(k: String, g: Array[Byte]): Int = g.length
  }
  val cache = CacheBuilder.newBuilder().maximumWeight(weight).weigher(weigher).build[String, Array[Byte]]()
}