package index

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

object Query {

  def prettyPrint(root: Option[String])(implicit cache: Cache, ec: ExecutionContext): Future[(Int, Int)] = {

    val levels = scala.collection.mutable.Map[Int, scala.collection.mutable.ArrayBuffer[Block]]()
    var num_data_blocks = 0

    def inOrder(start: Block, level: Int): Future[Unit] = {

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

          Future.successful({})

        case meta: Meta =>
          l += meta

          Future.traverse(meta.inOrder()){ case (_, id) =>
            cache.get[Block](id).flatMap(b => inOrder(b.get, level + 1))
          }.map(_ => {})
      }
    }

    (root match {
      case Some(root) => cache.get[Block](root).flatMap{b => inOrder(b.get, 0)}
      case _ => Future.successful()
    }).map { _ =>

      println("BEGIN BTREE:\n")
      levels.keys.toSeq.sorted.foreach { case level =>
        println(s"level[$level]: ${levels(level)}\n")
      }
      println("END BTREE\n")

      levels.size -> num_data_blocks
    }
  }

  def first(s: Option[String])(implicit ctx: Context, ec: ExecutionContext): Future[Option[Leaf]] = {
    s match {
      case None => Future.successful(None)
      case Some(id) => ctx.get[Block](id).flatMap {
        case None => Future.successful(None)
        case Some(block) => block match {
          case b: Leaf => Future.successful(Some(b))
          case b: Meta => getLeftMost(b)
        }
      }
    }
  }

  def last(s: Option[String])(implicit ctx: Context, ec: ExecutionContext): Future[Option[Leaf]] = {
    s match {
      case None => Future.successful(None)
      case Some(id) => ctx.get[Block](id).flatMap {
        case None => Future.successful(None)
        case Some(block) => block match {
          case b: Leaf => Future.successful(Some(b))
          case b: Meta => getRightMost(b)
        }
      }
    }
  }

  protected def getLeftMost(meta: Meta)(implicit ctx: Context, ec: ExecutionContext): Future[Option[Leaf]] = {
    meta.setPointers()

    ctx.get[Block](meta.pointers(0)._2).flatMap {
      case None => Future.successful(None)
      case Some(b) => b match {
        case leaf: Leaf => Future.successful(Some(leaf))
        case meta: Meta => getLeftMost(meta)
      }
    }
  }

  protected def grandpaNext(block: Meta)(implicit ctx: Context, ec: ExecutionContext): Future[Option[Leaf]] = {
    val (pid, pos) = ctx.parents(block.id)

    pid match {
      case None => Future.successful(None)
      case Some(id) => ctx.get[Meta](id).flatMap {
        case None => Future.successful(None)
        case Some(parent) =>

          parent.setPointers()

          if(pos + 1 < parent.pointers.length){
            ctx.get[Meta](parent.pointers(pos + 1)._2).flatMap {
              case None => Future.successful(None)
              case Some(meta) => getLeftMost(meta)
            }
          } else {
            grandpaNext(parent)
          }
      }
    }
  }

  def next(block: Leaf)(implicit ctx: Context, ec: ExecutionContext): Future[Option[Leaf]] = {
    val (pid, pos) = ctx.parents(block.id)

    pid match {
      case None => Future.successful(None)
      case Some(id) => ctx.get[Meta](id).flatMap {
        case None => Future.successful(None)
        case Some(parent) =>

          parent.setPointers()

          if(pos + 1 < parent.pointers.length){
            ctx.get[Leaf](parent.pointers(pos + 1)._2)
          } else {
            grandpaNext(parent)
          }
      }
    }
  }

  protected def getRightMost(meta: Meta)(implicit ctx: Context, ec: ExecutionContext): Future[Option[Leaf]] = {
    meta.setPointers()

    ctx.get[Block](meta.pointers(meta.pointers.length - 1)._2).flatMap {
      case None => Future.successful(None)
      case Some(b) => b match {
        case leaf: Leaf => Future.successful(Some(leaf))
        case meta: Meta => getRightMost(meta)
      }
    }
  }

  protected def grandpaPrevious(block: Meta)(implicit ctx: Context, ec: ExecutionContext): Future[Option[Leaf]] = {
    val (pid, pos) = ctx.parents(block.id)

    pid match {
      case None => Future.successful(None)
      case Some(id) => ctx.get[Meta](id).flatMap {
        case None => Future.successful(None)
        case Some(parent) =>
          parent.setPointers()

          if(pos - 1 >= 0){
            ctx.get[Meta](parent.pointers(pos - 1)._2).flatMap {
              case None => Future.successful(None)
              case Some(meta) => getRightMost(meta)
            }
          } else {
            grandpaPrevious(parent)
          }
      }
    }
  }

  def previous(block: Leaf)(implicit ctx: Context, ec: ExecutionContext): Future[Option[Leaf]] = {
    val (pid, pos) = ctx.parents(block.id)

    pid match {
      case None => Future.successful(None)
      case Some(id) => ctx.get[Meta](id).flatMap {
        case None => Future.successful(None)
        case Some(parent) =>

          parent.setPointers()

          if(pos - 1 >= 0){
            ctx.get[Leaf](parent.pointers(pos - 1)._2)
          } else {
            grandpaPrevious(parent)
          }
      }
    }
  }

  def inOrder(start: Option[String], root: Option[String])(implicit ec: ExecutionContext, cache: Cache): Future[Seq[Tuple]] = {
    start match {
      case None => Future.successful(Seq.empty[Tuple])
      case Some(id) => cache.get[Block](id).flatMap {
        case None => Future.successful(Seq.empty[Tuple])
        case Some(block) => block match {
          case leaf: Leaf =>

            if((root.isDefined && !leaf.id.equals(root.get))){

              //println(s"leaf min => ${leaf.length}/${leaf.MIN_LENGTH}")

              assert(leaf.hasMinimum())

              //println(s"leaf max => ${leaf.length}/${leaf.MAX_SIZE}")

              assert(leaf.length <= leaf.MAX_SIZE)
            }

            Future.successful(leaf.inOrder())

            case meta: Meta =>

              if((root.isDefined && !meta.id.equals(root.get))){

                //println(s"meta min => ${meta.length}/${meta.MIN_LENGTH}")

                assert(meta.hasMinimum())

                //println(s"meta max => ${meta.length}/${meta.MAX_SIZE}")

                assert(meta.length <= meta.MAX_SIZE)
              }

              Future.foldLeft(meta.inOrder().map{case (_, b) => inOrder(Some(b), root)})(Seq.empty[Tuple]){ case (b, n) =>
                b ++ n
              }
        }
      }
    }
  }

}
