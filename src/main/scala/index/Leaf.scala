package index

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

class Leaf(override val id: String,
           override val MIN: Int,
           override val MAX: Int)(implicit ord: Ordering[Bytes]) extends Block {

  var tuples = ArrayBuffer.empty[Tuple]

  var next: Option[String] = None
  var prev: Option[String] = None

  def find(k: Bytes, start: Int, end: Int): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = ord.compare(k, tuples(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return find(k, start, pos - 1)

    find(k, pos + 1, end)
  }

  def insert(data: Seq[Tuple]): (Boolean, Int) = {
    val slice = data.slice(0, Math.min(MAX - length, data.length))

    if(slice.exists{case (k, _) => tuples.exists{case (k1, _) => ord.equiv(k, k1)}}){
      return false -> 0
    }

    tuples = (tuples ++ slice).sortBy(_._1)

    true -> slice.length
  }

  def remove(data: Seq[Bytes]): (Boolean, Int) = {
    if(isEmpty()) return false -> 0

    if(data.exists{k1 => !tuples.exists{case (k, _) => ord.equiv(k, k1)}}){
      return false -> 0
    }

    tuples = tuples.filterNot{case (k, _) => data.exists(ord.equiv(_, k))}

    true -> data.length
  }

  def update(data: Seq[Tuple]): (Boolean, Int) = {
    if(data.exists{case (k, _) => !tuples.exists{case (k1, _) => ord.equiv(k, k1)}}){
      return false -> 0
    }

    tuples = tuples.filterNot{case (k, _) => data.exists{case (k1, _) => ord.equiv(k, k1)}}
    tuples = (tuples ++ data).sortBy(_._1)

    true -> data.length
  }

  override def copy()(implicit ctx: Context): Leaf = {
    if(ctx.blocks.isDefinedAt(id)) return this

    val copy = new Leaf(UUID.randomUUID.toString, MIN, MAX)

    ctx.blocks += copy.id -> copy
    ctx.parents += copy.id -> ctx.parents(id)

    copy.tuples = tuples.map {_.copy()}

    copy
  }

  override def split()(implicit ctx: Context): Leaf = {
    val right = new Leaf(UUID.randomUUID.toString, MIN, MAX)

    ctx.blocks += right.id -> right

    val pos = tuples.length/2

    right.tuples = tuples.slice(pos, tuples.length)
    tuples = tuples.slice(0, pos)

    right
  }

  override def canBorrowTo(target: Block)(implicit ctx: Context): Boolean = length - (MIN - target.length) >= MIN

  override def borrowLeftTo(t: Block)(implicit ctx: Context): Block = {
    val target = t.asInstanceOf[Leaf]
    val n = MIN - target.length
    val start = length - n

    target.tuples = tuples.slice(start, length) ++ target.tuples
    tuples = tuples.slice(0, start)

    target
  }

  override def borrowRightTo(t: Block)(implicit ctx: Context): Block = {
    val target = t.asInstanceOf[Leaf]
    val n = MIN - target.length
    val len = length

    target.tuples = target.tuples ++ tuples.slice(0, n)
    tuples = tuples.slice(n, len)

    target
  }

  override def merge(right: Block)(implicit ctx: Context): Block = {
    tuples = tuples ++ right.asInstanceOf[Leaf].tuples
    this
  }

  override def last: Bytes = tuples.last._1

  override def length: Int = tuples.length
  override def size: Int = tuples.map{case (k, v) => k.length + v.length}.sum

  override def isFull(): Boolean = tuples.length == MAX
  override def hasMinimum(): Boolean = tuples.length >= MIN
  override def isEmpty(): Boolean = tuples.isEmpty
  def inOrder(): Seq[Tuple] = tuples.toSeq

  override def toString: String = {
    "["+tuples.map{case (k, v) => new String(k) -> new String(v)}.mkString(",")+"]"
  }
}
