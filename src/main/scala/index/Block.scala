package index

trait Block {

  val id: String

  val MIN_LENGTH: Int
  val MAX_SIZE: Int
  val MAX_TUPLE_SIZE: Int

  def length: Int
  def size: Int
  def remaining: Int

  def last: Bytes
  //def copy()(implicit ctx: Context): this.type
  //def split()(implicit ctx: Context): this.type

  def isFull(): Boolean
  def hasMinimum(): Boolean
  def isEmpty(): Boolean

}
