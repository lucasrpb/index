package index

import scala.collection.concurrent.TrieMap

class MemoryCache extends Cache {
  val store = TrieMap.empty[String, Block]

  override def get(id: String): Block = {
    store(id)
  }

  override def save(ctx: Context): Boolean = {
    ctx.blocks.foreach { case (k, b) =>
      store.put(k, b)
    }
    true
  }
}
