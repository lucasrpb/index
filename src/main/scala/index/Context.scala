package index

import scala.collection.concurrent.TrieMap

class Context(var root: Option[String], val cache: Cache) {

  val blocks = TrieMap.empty[String, Block]
  val parents = TrieMap.empty[String, (Option[String], Int)]

  def get(id: String): Block = {
    blocks.get(id) match {
      case None => cache.get(id)
      case Some(block) => block
    }
  }

  def getLeaf(id: String): Leaf = {
    get(id).asInstanceOf[Leaf]
  }

  def getMeta(id: String): Meta = {
    get(id).asInstanceOf[Meta]
  }

}
