package ca.stevenskelton.thrift.testservice

import com.twitter.util.Future
import java.util.concurrent.atomic.AtomicInteger

class TestService extends TestApi.FutureIface {

	private def result(id: Int, msDelay: Int, counter: AtomicInteger) = {
		counter.incrementAndGet
		if (msDelay > 0) Thread.sleep(200)
		id match {
			case 1 | 10 | 100 => Future.value(SampleStruct(id, s"Id$id"))
			case 2 => throw NotFoundException(s"Id Not Found: $id")
			case 3 => throw DisabledException(s"Id Disabled: $id")
			case _ => ???
		}
	}

	val wNoDelayCount = new AtomicInteger(0)
	def wNoDelay(id: Int): Future[SampleStruct] = result(id, 0, wNoDelayCount)

	val w100msDelay = new AtomicInteger(0)
	def w100msDelay(id: Int): Future[SampleStruct] = result(id, 100, w100msDelay)

	val w200msDelay = new AtomicInteger(0)
	def w200msDelay(id: Int): Future[SampleStruct] = result(id, 200, w200msDelay)

	val w1sDelay = new AtomicInteger(0)
	def w1sDelay(id: Int): Future[SampleStruct] = result(id, 1000, w1sDelay)
}