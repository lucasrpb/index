package index

import scala.reflect.ClassTag

class Index[K: ClassTag, V: ClassTag](override val MIN: Int, override val MAX: Int)
                                     (implicit val ord: Ordering[K]) extends Block[K, V]{

  def insertNoPartition(data: Seq[(K, V)]): (Boolean, Int) = {
    val p = new Leaf[K, V](MIN, MAX)

    val (ok, n) = p.insert(data)

    if(!ok) return false -> 0

    true -> n
  }

  override def insert(data: Seq[(K, V)]): (Boolean, Int) = {
    val sorted = data.sortBy(_._1)

    val size = sorted.length
    var pos = 0

    while(pos < size){

      var list = sorted.slice(pos, size)
      val (k, _) = list(0)

      val (ok, n) = find(k) match {
        case None => insertNoPartition(list)
        case Some(p) =>

          val idx = list.indexWhere{case (k, _) => ord.gt(k, p.last)}
          if(idx > 0) list = list.slice(0, idx)

          insert(p, list)
      }

      if(!ok) return false -> 0

      pos += n
    }

    println(s"[insert] pos ${pos} size $size\n")

    true -> size
  }

}
