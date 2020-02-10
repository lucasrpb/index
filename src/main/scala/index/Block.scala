package index

trait Block {

  val id: String

  val MIN: Int
  val MAX: Int

  def last: Bytes
  //def copy()(implicit ctx: Context): this.type
  //def split()(implicit ctx: Context): this.type

  def isFull(): Boolean
  def isEmpty(): Boolean

}
