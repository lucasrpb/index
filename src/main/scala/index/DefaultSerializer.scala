package index
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

class DefaultSerializer(val NR_ENTRIES: Int)(implicit ord: Ordering[Bytes]) extends Serializer {

  val MAX = NR_ENTRIES
  val MIN = MAX/2

  val ID_SIZE = 36
  val BLOCK_SIZE_SIZE = 4
  val LENGTH_SIZE = 4
  val HEAD_SIZE = ID_SIZE + BLOCK_SIZE_SIZE + LENGTH_SIZE

  val KEY_SIZE = 4
  val VAL_SIZE = 4
  val TUPLE_HEAD_SIZE = KEY_SIZE + VAL_SIZE

  override def serialize(b: Leaf): ByteBuffer = {
    val id = b.id
    val size = b.size
    val len = b.length
    val buf = ByteBuffer.allocateDirect(HEAD_SIZE + len * TUPLE_HEAD_SIZE + b.size)

    buf.put(id.getBytes())
    buf.putInt(size)
    buf.putInt(len)

    b.tuples.foreach { case (k, v) =>
      buf.putInt(k.length)
      buf.putInt(v.length)

      buf.put(k)
      buf.put(v)
    }

    buf.flip()

    buf
  }

  override def serialize(b: Meta): ByteBuffer = {
    val id = b.id
    val size = b.size
    val len = b.length
    val buf = ByteBuffer.allocateDirect(HEAD_SIZE + len * TUPLE_HEAD_SIZE + b.size)

    buf.put(id.getBytes())
    buf.putInt(size)
    buf.putInt(len)

    b.pointers.foreach { case (k, v) =>
      buf.putInt(k.length)
      buf.putInt(v.length)

      buf.put(k)
      buf.put(v.getBytes)
    }

    buf.flip()

    buf
  }

  override def deserializeLeaf(buf: ByteBuffer): Leaf = {
    val id = Array.ofDim[Byte](ID_SIZE)
    buf.get(id)

    val leaf = new Leaf(new String(id), MIN, MAX)

    val size = buf.getInt()
    val len = buf.getInt()

    val builder = ArrayBuffer.newBuilder[Tuple]
    builder.sizeHint(len)

    for(i<-0 until len){
      val kl = buf.getInt()
      val vl = buf.getInt()

      val k_arr = Array.ofDim[Byte](kl)
      val v_arr = Array.ofDim[Byte](vl)

      buf.get(k_arr)
      buf.get(v_arr)

      builder += k_arr -> v_arr
    }

    leaf.tuples = builder.result()

    leaf
  }

  override def deserializeMeta(buf: ByteBuffer): Meta = {
    val id = Array.ofDim[Byte](ID_SIZE)
    buf.get(id)

    val meta = new Meta(new String(id), MIN, MAX)

    // size...
    buf.getInt()

    //length...
    val len = buf.getInt()

    val builder = ArrayBuffer.newBuilder[Pointer]
    builder.sizeHint(len)

    for(i<-0 until len){
      val kl = buf.getInt()
      val vl = buf.getInt()

      val k_arr = Array.ofDim[Byte](kl)
      val v_arr = Array.ofDim[Byte](vl)

      buf.get(k_arr)
      buf.get(v_arr)

      builder += k_arr -> new String(v_arr)
    }

    meta.pointers = builder.result()

    meta
  }
}
