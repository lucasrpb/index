package index

object Query {

  def prettyPrint(root: Option[String])(implicit cache: Cache): (Int, Int) = {

    val levels = scala.collection.mutable.Map[Int, scala.collection.mutable.ArrayBuffer[Block]]()
    var num_data_blocks = 0

    def inOrder(start: Block, level: Int): Unit = {

      val opt = levels.get(level)
      var l: scala.collection.mutable.ArrayBuffer[Block] = null

      if(opt.isEmpty){
        l = scala.collection.mutable.ArrayBuffer[Block]()
        levels  += level -> l
      } else {
        l = opt.get
      }

      start match {
        case data: Leaf =>
          num_data_blocks += 1
          l += data

        case meta: Meta =>

          l += meta

          val length = meta.pointers.length
          val pointers = meta.pointers

          for(i<-0 until length){
            inOrder(cache.get(pointers(i)._2), level + 1)
          }

      }
    }

    root match {
      case Some(root) => inOrder(cache.get(root), 0)
      case _ =>
    }

    println("BEGIN BTREE:\n")
    levels.keys.toSeq.sorted.foreach { case level =>
      println(s"level[$level]: ${levels(level)}\n")
    }
    println("END BTREE\n")

    levels.size -> num_data_blocks
  }



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
