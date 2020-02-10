package index

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference

import com.google.common.primitives.UnsignedBytes
import org.scalatest.flatspec.AnyFlatSpec

class MainSpec extends AnyFlatSpec {

  "index data " must "be equal to list data" in {

    val rand = ThreadLocalRandom.current()

    implicit val ord = new Ordering[Bytes] {
      val c = UnsignedBytes.lexicographicalComparator()
      override def compare(x: Bytes, y: Bytes): Int = c.compare(x, y)
    }

    implicit val cache = new MemoryCache()

    val n = 20000

    var list = Seq.empty[(Bytes, Bytes)]

    for(i<-0 until n){
      val e = rand.nextInt(10000, 99999).toString.getBytes()

      if(!list.exists{case (k, _) => ord.equiv(k, e)}){
        list = list :+ e -> e
      }
    }

    val ref = new AtomicReference[Option[String]](None)
    val index = new Index(ref.get(), 10, 100)

    if(index.insert(list)._1){
      cache.save(index.ctx)
      ref.set(index.ctx.root)
    }

    val slist = list.sortBy(_._1).map(_._1)
    val ilist = Query.inOrder(ref.get(), ref.get()).map(_._1)

    assert(ilist.equals(slist))

    println(s"list: ${slist.map{new String(_)}}\n")
    println(s"ilist: ${ilist.map{new String(_)}}\n")
  }

}
