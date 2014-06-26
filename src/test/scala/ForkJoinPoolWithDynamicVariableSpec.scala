/**
 * Part of a blog entry at: http://stevenskelton.ca/threadlocal-variables-scala-futures/
 */
import org.specs2._
import org.specs2.matcher._
import scala.util.DynamicVariable
import scala.concurrent._
import java.util.UUID

class ForkJoinPoolWithDynamicVariableSpec extends mutable.SpecificationWithJUnit {

  sequential
  val duration = scala.concurrent.duration.Duration.apply(1, scala.concurrent.duration.SECONDS)

  case class MyContext(uuid: UUID = UUID.randomUUID, fields: Map[String, String] = Map.empty)

  "work with case class" in {

    val dyn = new DynamicVariable[MyContext](MyContext())

    def doSomething(str: String) {
      println {
        val context = dyn.value
        str + " " + Thread.currentThread().getName() + "  " + context.fields.get("fut") + "   uuid: " + context.uuid.toString
      }
    }

    implicit val executorService: ExecutionContext = scala.concurrent.ExecutionContext.fromExecutorService(new ForkJoinPoolWithDynamicVariable(dyn))

    val fut1 = Future {
      dyn.withValue(MyContext(fields = Map("fut" -> "1"))) { doSomething("fut1") }
    }
    val fut2 = Future {
      dyn.withValue(MyContext(fields = Map("fut" -> "2"))) { doSomething("fut2") }
    }
    val fut3 = dyn.withValue(MyContext(fields = Map("fut" -> "3"))) {
      Future {
        doSomething("fut3")
      }
    }
    val fut4 = dyn.withValue(MyContext(fields = Map("fut" -> "4"))) {
      Future {
        doSomething("fut4")
      }
    }
    val fut5 = dyn.withValue(MyContext(fields = Map("fut" -> "5"))) {
      Future {
        doSomething("fut5")
      }
    }

    import scala.concurrent.Await
    Await.result(fut1, duration)
    Await.result(fut2, duration)
    Await.result(fut3, duration)
    Await.result(fut4, duration)
    Await.result(fut5, duration)

    success
  }
  "do Future chaining" in {

    //create a dynamic variable
    val dyn = new DynamicVariable[Int](0)

    def printThreadInfo(id: String) = println {
      id + " : " + Thread.currentThread.getName + " = " + dyn.value
    }

    implicit val executionContext = scala.concurrent.ExecutionContext.fromExecutorService(new ForkJoinPoolWithDynamicVariable(dyn))

    val fut1 = dyn.withValue(1) { Future { printThreadInfo("fut1") } }
    val fut2 = dyn.withValue(2) { Future { printThreadInfo("fut2") } }
    val fut3 = dyn.withValue(3) { Future { printThreadInfo("fut3") } }

    Await.result(fut1, duration)
    Await.result(fut2, duration)
    Await.result(fut3, duration)

    val fut4 = dyn.withValue(4) { Future { Thread.sleep(100); 2 } }
    val fut5 = dyn.withValue(5) { Future { printThreadInfo("fut5") } }

    val fut4_c = fut4.map { v => printThreadInfo("fut4_b"); Thread.sleep(100); v + 1 }
      .map { v => Thread.sleep(100); printThreadInfo("fut4_c"); v + 1 }

    val fut6 = dyn.withValue(6) { Future { printThreadInfo("fut6") } }
    val fut7 = dyn.withValue(7) { Future { printThreadInfo("fut7") } }
    val fut8 = dyn.withValue(8) { Future { printThreadInfo("fut8") } }

    Await.result(fut5, duration)
    Await.result(fut6, duration)
    Await.result(fut7, duration)
    Await.result(fut8, duration)
    Await.result(fut4_c, duration) === 4
  }
}