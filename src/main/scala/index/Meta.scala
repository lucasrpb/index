package index

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

class Meta(override val id: String,
           override val MIN: Int,
           override val MAX: Int)(implicit val ord: Ordering[Bytes]) extends Block {

  var pointers = ArrayBuffer.empty[Pointer]

  def find(k: Bytes, start: Int, end: Int): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = ord.compare(k, pointers(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return find(k, start, pos - 1)

    find(k, pos + 1, end)
  }

  def findPath(k: Bytes): Option[String] = {
    if(isEmpty()) return None
    val (_, pos) = find(k, 0, pointers.length - 1)
    Some(pointers(if(pos < pointers.length) pos else pos - 1)._2)
  }

  def left(pos: Int): Option[String] = {
    val lpos = pos - 1
    if(lpos < 0) return None
    Some(pointers(lpos)._2)
  }

  def right(pos: Int): Option[String] = {
    val rpos = pos + 1
    if(rpos >= pointers.length) return None
    Some(pointers(rpos)._2)
  }

  def setPointer(ptrs: Seq[Tuple3[Array[Byte], String, Int]])(implicit ctx: Context): Unit = {
    ptrs.foreach { case (k, c, pos) =>
      pointers(pos) = k -> c
      ctx.parents += c -> (Some(id), pos)
    }
  }

  def setPointers()(implicit ctx: Context): Unit = {
    for(i<-0 until pointers.length){
      val (_, child) = pointers(i)
      ctx.parents += child -> (Some(id), i)
    }
  }

  def insert(data: Seq[Pointer])(implicit ctx: Context): (Boolean, Int) = {
    if(isFull()) return false -> 0

    val n = Math.min(data.length, MAX - pointers.length)
    val slice = data.slice(0, n)

    if(slice.exists{case (k, _) => pointers.exists{case (k1, _) => ord.equiv(k, k1)}}){
      return false -> 0
    }

    pointers = (pointers ++ slice).sortBy(_._1)

    setPointers()

    true -> n
  }

  def remove(data: Seq[Bytes])(implicit ctx: Context): (Boolean, Int) = {
    if(isEmpty()) return false -> 0

    if(data.exists{k1 => !pointers.exists{case (k, _) => ord.equiv(k, k1)}}){
      return false -> 0
    }

    pointers = pointers.filterNot{case (k, _) => data.exists(ord.equiv(_, k))}

    setPointers()

    true -> data.length
  }

  def removeAt(pos: Int)(implicit ctx: Context): Pointer = {
    val p = pointers.remove(pos)
    setPointers()
    p
  }

  def update(data: Seq[Pointer])(implicit ctx: Context): (Boolean, Int) = {

    val len = pointers.length

    for(i<-0 until data.length){
      val (k, child) = data(i)

      val (found, idx) = find(k, 0, len - 1)

      if(!found) return false -> 0

      setPointer(Seq(Tuple3(k, child, idx)))
    }

    true -> data.length
  }

  override def isFull(): Boolean = pointers.length == MAX
  override def hasMinimum(): Boolean = pointers.length >= MIN
  override def isEmpty(): Boolean = pointers.isEmpty

  override def last: Bytes = pointers.last._1

  def copy()(implicit ctx: Context): Meta = {
    if(ctx.blocks.isDefinedAt(id)) return this

    val copy = new Meta(UUID.randomUUID.toString, MIN, MAX)

    ctx.blocks += copy.id -> copy
    ctx.parents += copy.id -> ctx.parents(id)

    copy.pointers = pointers.map {_.copy()}
    copy.setPointers()

    copy
  }

  def split()(implicit ctx: Context): Meta = {
    val right = new Meta(UUID.randomUUID.toString, MIN, MAX)

    ctx.blocks += right.id -> right

    val pos = pointers.length/2

    right.pointers = pointers.slice(pos, pointers.length)
    pointers = pointers.slice(0, pos)

    setPointers()
    right.setPointers()

    right
  }

  def canBorrowTo(target: Meta): Boolean = pointers.length - (MIN - target.pointers.length) >= MIN

  def borrowLeftTo(target: Meta)(implicit ctx: Context): Meta = {
    val n = MIN - target.pointers.length
    val start = pointers.length - n

    target.pointers = pointers.slice(start, pointers.length) ++ target.pointers
    pointers = pointers.slice(0, start)

    setPointers()
    target.setPointers()

    target
  }

  def borrowRightTo(target: Meta)(implicit ctx: Context): Meta = {
    val n = MIN - target.pointers.length
    val len = pointers.length

    target.pointers = target.pointers ++ pointers.slice(0, n)
    pointers = pointers.slice(n, len)

    setPointers()
    target.setPointers()

    target
  }

  def merge(right: Meta)(implicit ctx: Context): Meta = {
    pointers = pointers ++ right.pointers
    setPointers()
    this
  }

  def inOrder(): Seq[Pointer] = pointers.toSeq

  override def toString: String = {
    "["+pointers.map{case (k, _) => new String(k)}.mkString(",")+"]"
  }
}
