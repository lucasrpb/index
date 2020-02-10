package index

import java.util.UUID

class Index(val root: Option[String],
            val MIN: Int,
            val MAX: Int)(implicit val ord: Ordering[Bytes], cache: Cache){

  implicit val ctx = new Context(root, cache)

  def find(k: Bytes, root: Option[String] = ctx.root): Option[Leaf] = {
    root match {
      case None => None
      case Some(root) => ctx.get(root) match {
        case leaf: Leaf => Some(leaf)
        case meta: Meta => find(k, meta.findPath(k))
      }
    }
  }

  def recursiveCopy(p: Block): Boolean = {
    val (pid, pos) = ctx.parents(p.id)

    pid match {
      case None =>
        ctx.root = Some(p.id)
        ctx.parents += p.id -> (None, 0)
        true

      case Some(pid) =>
        val parent = ctx.getMeta(pid).copy()
        parent.setPointer(p.last, p.id, pos)

        recursiveCopy(parent)
    }
  }

  def insertEmpty(data: Seq[Tuple]): (Boolean, Int) = {
    val leaf = new Leaf(UUID.randomUUID.toString, MIN, MAX)

    ctx.blocks += leaf.id -> leaf
    ctx.parents += leaf.id -> (None, 0)

    val (ok, n) = leaf.insert(data)

    if(!ok) return false -> 0

    recursiveCopy(leaf) -> n
  }

  def insertParent(left: Meta, prev: Block): Boolean = {
    if(left.isFull()){
      val right = left.split()

      if(ord.gt(prev.last, left.last)){
        right.insert(Seq(prev.last -> prev.id))
      } else {
        left.insert(Seq(prev.last -> prev.id))
      }

      return handleParent(left, right)
    }

    left.insert(Seq(prev.last -> prev.id))._1

    recursiveCopy(left)
  }

  def handleParent(left: Block, right: Block): Boolean = {
    val (pid, pos) = ctx.parents(left.id)

    pid match {
      case None =>

        val r = new Meta(UUID.randomUUID.toString, MIN, MAX)

        ctx.blocks += r.id -> r
        ctx.parents += r.id -> (None, 0)

        r.insert(Seq(
          left.last -> left.id,
          right.last -> right.id
        ))

        recursiveCopy(r)

      case Some(pid) =>
        val parent = ctx.getMeta(pid).copy()

        parent.setPointer(left.last, left.id, pos)
        ctx.parents += left.id -> (Some(parent.id), pos)

        insertParent(parent, right)
    }
  }

  def insert(leaf: Leaf, data: Seq[Tuple]): (Boolean, Int) = {

    val left = leaf.copy()

    if(left.isFull()){
      val right = left.split()
      return handleParent(left, right) -> 0
    }

    val (ok, n) = left.insert(data)

    if(!ok) return false -> 0

    recursiveCopy(left) -> n
  }

  def insert(data: Seq[Tuple]): (Boolean, Int) = {
    val sorted = data.sortBy(_._1)

    val size = sorted.length
    var pos = 0

    while(pos < size){

      var list = sorted.slice(pos, size)
      val (k, _) = list(0)

      val (ok, n) = find(k) match {
        case None => insertEmpty(list)
        case Some(p) =>

          val idx = list.indexWhere{case (k, _) => ord.gt(k, p.last)}
          if(idx > 0) list = list.slice(0, idx)

          insert(p, list)
      }

      if(!ok) return false -> 0

      pos += n
    }

    true -> size
  }

}
