/**
 * Part of a blog entry at: http://stevenskelton.ca/threadlocal-variables-scala-futures/
 */
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import scala.concurrent.forkjoin._
import scala.util.DynamicVariable

class ForkJoinPoolWithDynamicVariable[T](dynamicVariable: DynamicVariable[T]) extends ForkJoinPool {
  override def execute(task: Runnable) {
    val copyValue = dynamicVariable.value
    super.execute(new Runnable {
      override def run = {
        dynamicVariable.value = copyValue
        task.run
      }
    })
  }
}
object Main extends App {

  //create a dynamic variable
  val dyn = new DynamicVariable[Int](0)

  //use our new execution context
  implicit val executionContext = scala.concurrent.ExecutionContext.fromExecutorService(new ForkJoinPoolWithDynamicVariable(dyn))

  //prints thread information and dynamic variable value.
  def printThreadInfo(id: String) = println {
    id + " : " + Thread.currentThread.getName + " = " + dyn.value
  }

  val fut1 = dyn.withValue(1) { Future { printThreadInfo("fut1") } }
  val fut2 = dyn.withValue(2) { Future { printThreadInfo("fut2") } }
  val fut3 = dyn.withValue(3) { Future { printThreadInfo("fut3") } }

  Await.result(fut1, 1.second)
  Await.result(fut2, 1.second)
  Await.result(fut3, 1.second)

  val fut4 = dyn.withValue(4) { Future { printThreadInfo("fut4") } }
  val fut5 = dyn.withValue(5) { Future { printThreadInfo("fut5") } }

  Await.result(fut4, 1.second)
  Await.result(fut5, 1.second)
}
