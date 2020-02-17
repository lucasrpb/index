package index

trait Block {

  type T <: Block

  val id: String

  val MIN_LENGTH: Int
  val MAX_SIZE: Int
  val MAX_TUPLE_SIZE: Int

  def length: Int
  def size: Int
  def remaining: Int

  def last: Bytes

  def split()(implicit ctx: Context): T
  def copy()(implicit ctx: Context): T

  def canBorrowTo(target: Block)(implicit ctx: Context): Boolean
  def borrowLeftTo(target: Block)(implicit ctx: Context): T
  def borrowRightTo(target: Block)(implicit ctx: Context): T
  def merge(right: Block)(implicit ctx: Context): T

  def isFull(): Boolean
  def hasMinimum(): Boolean
  def isEmpty(): Boolean

}
