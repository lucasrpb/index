package index

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class Context(var root: Option[String])(implicit val ec: ExecutionContext, cache: Cache) {

  val blocks = TrieMap.empty[String, Block]
  val parents = TrieMap.empty[String, (Option[String], Int)]

  root match {
    case Some(id) => parents.put(id, None -> 0)
    case _ =>
  }

  def get(id: String): Future[Option[Block]] = {
    blocks.get(id) match {
      case None => cache.get(id)
      case Some(block) => Future.successful(Some(block))
    }
  }

  def getLeaf(id: String): Future[Option[Leaf]] = {
    get(id).map(_.map(_.asInstanceOf[Leaf]))
  }

  def getMeta(id: String): Future[Option[Meta]] = {
    get(id).map(_.map(_.asInstanceOf[Meta]))
  }

}
