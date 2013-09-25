/**
 * Part of a blog entry at: http://stevenskelton.ca/separation-of-concerns-with-finagle/
 */
import com.twitter.finagle.{ Service, SimpleFilter }
import com.twitter.util.Future
/**
 * Invokes post concern on successful service response.
 */
class PostConcernFilter(postConcernService: Service[Array[Byte], Array[Byte]]) extends SimpleFilter[Array[Byte], Array[Byte]] {
  def apply(request: Array[Byte], service: Service[Array[Byte], Array[Byte]]): Future[Array[Byte]] = {
    service(request).onSuccess(_ => postConcernService(request))
  }
}