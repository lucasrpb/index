package index

import java.util.concurrent.{CompletableFuture, Executor}
import com.github.benmanes.caffeine.cache.{AsyncCacheLoader, Caffeine, RemovalCause}
import scala.compat.java8.FutureConverters
import scala.compat.java8.JFunction._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class DefaultCache(val store: Store)(implicit val ec: ExecutionContext) extends Cache {

  val MAX_SIZE = 110L * 1024L

  val cache = Caffeine.newBuilder()
    .weigher[String, Option[Block]]((key: String, value: Option[Block]) => {
      if(value == null || value.isEmpty) 0 else value.get.size
    })
    .executor(ec.asInstanceOf[Executor])
    .maximumWeight(MAX_SIZE)
    .removalListener((key: String, value: Option[Block], cause: RemovalCause) => {
      println(s"REMOVING ${key} FROM CACHE...\n")
    })
    .buildAsync(new AsyncCacheLoader[String, Option[Block]] {
      override def asyncLoad(key: String, executor: Executor): CompletableFuture[Option[Block]] = {
        val cf = new CompletableFuture[Option[Block]]()

        println(s"LOADING $key FROM CACHE...\n")

        store.get(key).onComplete {
          _ match {
            case Success(block) => cf.complete(block)
            case Failure(e) => cf.completeExceptionally(e)
          }
        }

        cf
      }
    })

  override def get[T <: Block](id: String): Future[Option[T]] = this.synchronized {
    FutureConverters.toScala(cache.get(id)).map(_.map(_.asInstanceOf[T]))
  }

  override def save(ctx: Context): Future[Boolean] = this.synchronized {
    val blocks = ctx.blocks

    store.save(ctx).flatMap { ok =>
      if(ok){
        Future.sequence(blocks.map{case (id, b) =>
          val cf = new CompletableFuture[Option[Block]]{
            this.complete(Some(b))
          }
          cache.put(id, cf)
          FutureConverters.toScala(cf)
        }).map { _ =>
          blocks.clear()
          true
        }
      } else {
        Future.successful(false)
      }
    }
  }

}
