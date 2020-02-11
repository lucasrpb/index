package index

import java.util.UUID
import scala.collection.mutable.ArrayBuffer

class Leaf(override val id: String,
           override val MIN: Int,
           override val MAX: Int)(implicit ord: Ordering[Bytes]) extends Block {

  var tuples = ArrayBuffer.empty[Tuple]

  def find(k: Bytes, start: Int, end: Int): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = ord.compare(k, tuples(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return find(k, start, pos - 1)

    find(k, pos + 1, end)
  }

  def insert(data: Seq[Tuple]): (Boolean, Int) = {
    if(isFull()) return false -> 0

    val n = Math.min(data.length, MAX - tuples.length)
    val slice = data.slice(0, n)

    if(slice.exists{case (k, _) => tuples.exists{case (k1, _) => ord.equiv(k, k1)}}){
      return false -> 0
    }

    tuples = (tuples ++ slice).sortBy(_._1)

    true -> n
  }

  def remove(data: Seq[Bytes]): (Boolean, Int) = {
    if(isEmpty()) return false -> 0

    if(data.exists{k1 => !tuples.exists{case (k, _) => ord.equiv(k, k1)}}){
      return false -> 0
    }

    tuples = tuples.filterNot{case (k, _) => data.exists(ord.equiv(_, k))}

    true -> data.length
  }

  def copy()(implicit ctx: Context): Leaf = {
    if(ctx.blocks.isDefinedAt(id)) return this

    val copy = new Leaf(UUID.randomUUID.toString, MIN, MAX)

    ctx.blocks += copy.id -> copy
    ctx.parents += copy.id -> ctx.parents(id)

    copy.tuples = tuples.map {_.copy()}

    copy
  }

  def split()(implicit ctx: Context): Leaf = {
    val right = new Leaf(UUID.randomUUID.toString, MIN, MAX)

    ctx.blocks += right.id -> right

    val pos = tuples.length/2

    right.tuples = tuples.slice(pos, tuples.length)
    tuples = tuples.slice(0, pos)

    right
  }

  override def last: Bytes = tuples.last._1

  override def isFull(): Boolean = tuples.length == MAX
  override def isEmpty(): Boolean = tuples.isEmpty
  def inOrder(): Seq[Tuple] = tuples.toSeq

  override def toString: String = {
    tuples.map{case (k, v) => new String(k) -> new String(v)}.mkString(",")
  }
}
