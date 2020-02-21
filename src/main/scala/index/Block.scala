package index

trait Block {

  val id: String

  val MIN: Int
  val MAX: Int

  def length: Int
  def size: Int

  def last: Bytes

  def split()(implicit ctx: Context): Block
  def copy()(implicit ctx: Context): Block

  def canBorrowTo(target: Block)(implicit ctx: Context): Boolean
  def borrowLeftTo(target: Block)(implicit ctx: Context): Block
  def borrowRightTo(target: Block)(implicit ctx: Context): Block
  def merge(right: Block)(implicit ctx: Context): Block

  def isFull(): Boolean
  def hasMinimum(): Boolean
  def isEmpty(): Boolean

}
