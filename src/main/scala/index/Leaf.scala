package index

import scala.reflect.ClassTag

class Leaf[K: ClassTag, V: ClassTag](override val MIN: Int, override val MAX: Int)(implicit ord: Ordering[K]) extends Block[K, V] {

  var tuples = Seq.empty[(K, V)]

  def find(k: K, start: Int, end: Int): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = ord.compare(k, tuples(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return find(k, start, pos - 1)

    find(k, pos + 1, end)
  }

  override def near(k: K): Option[V] = {
    if(isEmpty()) return None
    val (_, pos) = find(k, 0, tuples.length - 1)
    Some(tuples(if(pos < tuples.length) pos else pos - 1)._2)
  }

  override def insert(data: Seq[(K, V)]): (Boolean, Int) = {
    if(isFull()) return false -> 0

    val n = Math.min(data.length, MAX - tuples.length)
    val slice = data.slice(0, n)

    if(slice.exists{case (k, _) => tuples.exists{case (k1, _) => ord.equiv(k, k1)}}){
      return false -> 0
    }

    tuples = (tuples ++ slice).sortBy(_._1)

    true -> n
  }

  override def split(): Block[K, V] = {
    val right = new Leaf[K, V](MIN, MAX)

    val old = tuples
    val middle = tuples.length/2

    tuples = old.slice(0, middle)
    right.tuples = old.slice(middle, tuples.length)

    right
  }

  override def remove(data: Seq[K]): (Boolean, Int) = {
    if(isEmpty()) return false -> 0

    tuples = tuples.filterNot{case (k, _) => data.exists(ord.equiv(_, k))}

    true -> data.length
  }

  override def last: K = tuples.last._1

  override def isFull(): Boolean = tuples.length == MAX
  override def isEmpty(): Boolean = tuples.isEmpty
  override def inOrder(): Seq[(K, V)] = tuples

  override def hasMinimum(): Boolean = tuples.length >= MIN

  override def toString: String = {
    tuples.mkString(",")
  }
}
