package index

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class MemoryCache extends Cache {
  val store = TrieMap.empty[String, Block]

  override def get[T <: Block](id: String): Future[Option[T]] = {
    Future.successful(store.get(id).map(_.asInstanceOf[T]))
  }

  override def save(ctx: Context): Future[Boolean] = {
    ctx.blocks.foreach { case (k, b) =>
      store.put(k, b)
    }
    Future.successful(true)
  }
}
