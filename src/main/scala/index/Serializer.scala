package index

import java.nio.ByteBuffer

trait Serializer {

  def serialize(b: Leaf): ByteBuffer
  def serialize(b: Meta): ByteBuffer

  def deserializeLeaf(bytes: ByteBuffer): Leaf
  def deserializeMeta(bytes: ByteBuffer): Meta

}
