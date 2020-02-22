import java.util.concurrent.{CompletableFuture, Executor}
import java.util.function.BiFunction

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent.{ExecutionContext, Future, Promise}

package object index {

  type Bytes = Array[Byte]

  type Pointer = Tuple2[Bytes, String]
  type Tuple = Tuple2[Bytes, Bytes]

  implicit def toScalaFuture[T](lf: ListenableFuture[T])(implicit ec: ExecutionContext): Future[T] = {
    val p = Promise[T]

    Futures.addCallback(lf, new FutureCallback[T] {
      override def onSuccess(result: T): Unit = {
        p.success(result)
      }

      override def onFailure(t: Throwable): Unit = {
        p.failure(t)
      }
    }, ec.asInstanceOf[Executor])

    p.future
  }

  /*implicit def toScalaFuture[T](cf: CompletableFuture[T])(implicit ec: ExecutionContext): Future[T] = {
    val p = Promise[T]



    p.future
  }*/

}
