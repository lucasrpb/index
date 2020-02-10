package index

trait Block[K, V] {

  val MIN: Int
  val MAX: Int

  def last: K
  def split(): Block[K, V]

  def insert(data: Seq[(K, V)]): (Boolean, Int)
  def remove(data: Seq[K]): (Boolean, Int)

  def isFull(): Boolean
  def isEmpty(): Boolean
  def inOrder(): Seq[(K, V)]

}
