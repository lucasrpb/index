package index

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

object Query {

  /*def prettyPrint(root: Option[String])(implicit cache: Cache): (Int, Int) = {

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

  protected def getLeftMost(meta: Meta)(implicit ctx: Context): Option[Leaf] = {
    meta.setPointers()

    ctx.get(meta.pointers(0)._2) match {
      case b: Leaf => Some(b)
      case b: Meta => getLeftMost(b)
    }
  }

  protected def getRightMost(meta: Meta)(implicit ctx: Context): Option[Leaf] = {
    meta.setPointers()

    ctx.get(meta.pointers(meta.pointers.length - 1)._2) match {
      case b: Leaf => Some(b)
      case b: Meta => getRightMost(b)
    }
  }

  protected def grandpaNext(block: Meta)(implicit ctx: Context): Option[Leaf] = {
    val (pid, pos) = ctx.parents(block.id)

    pid match {
      case None => None
      case Some(id) =>
        val parent = ctx.getMeta(id)

        parent.setPointers()

        if(pos + 1 < parent.pointers.length){
          getLeftMost(ctx.getMeta(parent.pointers(pos + 1)._2))
        } else {
          grandpaNext(parent)
        }
    }
  }

  protected def grandpaPrevious(block: Meta)(implicit ctx: Context): Option[Leaf] = {
    val (pid, pos) = ctx.parents(block.id)

    pid match {
      case None => None
      case Some(id) =>
        val parent = ctx.getMeta(id)

        parent.setPointers()

        if(pos - 1 >= 0){
          getRightMost(ctx.getMeta(parent.pointers(pos - 1)._2))
        } else {
          grandpaPrevious(parent)
        }
    }
  }

  def first(s: Option[String])(implicit ctx: Context): Option[Leaf] = {
    s match {
      case None => None
      case Some(id) => ctx.get(id) match {
        case b: Leaf => Some(b)
        case b: Meta => getLeftMost(b)
      }
    }
  }

  def last(s: Option[String])(implicit ctx: Context): Option[Leaf] = {
    s match {
      case None => None
      case Some(id) => ctx.get(id) match {
        case b: Leaf => Some(b)
        case b: Meta => getRightMost(b)
      }
    }
  }

  def next(block: Leaf)(implicit ctx: Context): Option[Leaf] = {
    val (pid, pos) = ctx.parents(block.id)

    pid match {
      case None => None
      case Some(id) =>
        val parent = ctx.getMeta(id)

        parent.setPointers()

        if(pos + 1 < parent.pointers.length){
          Some(ctx.getLeaf(parent.pointers(pos + 1)._2))
        } else {
          grandpaNext(parent)
        }
    }
  }

  def previous(block: Leaf)(implicit ctx: Context): Option[Leaf] = {
    val (pid, pos) = ctx.parents(block.id)

    pid match {
      case None => None
      case Some(id) =>
        val parent = ctx.getMeta(id)

        parent.setPointers()

        if(pos - 1 >= 0){
          Some(ctx.getLeaf(parent.pointers(pos - 1)._2))
        } else {
          grandpaPrevious(parent)
        }
    }
  }*/

  /*def inOrder(start: Option[String], root: Option[String])(implicit ec: ExecutionContext, cache: Cache): Future[Seq[Tuple]] = {
    start match {
      case None => Future.successful(Seq.empty[Tuple])
      case Some(id) => cache.get(id).flatMap {
        case None => Future.successful(Seq.empty[Tuple])
        case Some(block) => block match {
          case leaf: Leaf =>

            if((root.isDefined && !leaf.id.equals(root.get))){
              assert(leaf.hasMinimum() && leaf.size <= leaf.MAX_SIZE)
            }

            Future.successful(leaf.inOrder())

            case meta: Meta =>

              if((root.isDefined && !meta.id.equals(root.get))){
                assert(meta.hasMinimum() && meta.size <= meta.MAX_SIZE)
              }

              Future.foldLeft(meta.inOrder().map{case (k, b) => inOrder(Some(b), root)})(Seq.empty[Tuple]){ case (b, n) =>
                b ++ n
              }
        }
      }
    }
  }*/

  def inOrder(start: Option[String], root: Option[String])(implicit cache: Cache, ec: ExecutionContext): Seq[Tuple] = {
    start match {
      case None => Seq.empty[Tuple]
      case Some(id) => Await.result(cache.get(id), 10 seconds).get match {
        case leaf: Leaf =>

          if((root.isDefined && !leaf.id.equals(root.get))){
            assert(leaf.hasMinimum() && leaf.size <= leaf.MAX_SIZE)
          }

          leaf.inOrder()
        case meta: Meta =>

          if((root.isDefined && !meta.id.equals(root.get))){

            assert(meta.hasMinimum() && meta.size <= meta.MAX_SIZE)
          }

          meta.inOrder().foldLeft(Seq.empty[Tuple]) { case (b, (_, n)) =>
            b ++ inOrder(Some(n), root)
          }
      }
    }
  }
}
