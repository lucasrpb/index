package index

object Query {
  def inOrder(start: Option[String], root: Option[String])(implicit cache: Cache): Seq[Tuple] = {
    start match {
      case None => Seq.empty[Tuple]
      case Some(id) => cache.get(id) match {
        case leaf: Leaf => leaf.inOrder()
        case meta: Meta => meta.inOrder().foldLeft(Seq.empty[Tuple]) { case (b, (_, n)) =>
            b ++ inOrder(Some(n), root)
          }
      }
    }
  }
}
