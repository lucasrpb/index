package index

trait Block {

 // type T = this.type

  val id: String

  val MIN_LENGTH: Int
  val MAX_LENGTH: Int
  val MAX_SIZE: Int
  val MAX_TUPLE_SIZE: Int

  def length: Int
  def size: Int
  def remaining: Int

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
