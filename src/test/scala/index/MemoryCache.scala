package index

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class MemoryCache extends Cache {
  val store = TrieMap.empty[String, Block]

  override def get(id: String): Future[Option[Block]] = {
    Future.successful(store.get(id))
  }

  override def save(ctx: Context): Future[Boolean] = {
    ctx.blocks.foreach { case (k, b) =>
      store.put(k, b)
    }
    Future.successful(true)
  }
}
