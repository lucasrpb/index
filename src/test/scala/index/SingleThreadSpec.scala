package index

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference

import com.google.common.primitives.UnsignedBytes
import org.apache.commons.lang3.{RandomStringUtils, StringUtils}
import org.scalatest.{Canceled, Failed, Outcome, Succeeded}
import org.scalatest.flatspec.AnyFlatSpec
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class SingleThreadSpec extends Retriable {

  override val times = 1

  "index data " must "be equal to list data" in {

    val rand = ThreadLocalRandom.current()

    implicit val ord = new Ordering[Bytes] {
      val c = UnsignedBytes.lexicographicalComparator()
      override def compare(x: Bytes, y: Bytes): Int = c.compare(x, y)
    }

    val ref = new AtomicReference[Option[String]](None)
    implicit val cache = new MemoryCache()
    var data = Seq.empty[Tuple]

    val iter = 1000
    val MIN = 2
    val MAX = 10

    def insert(): Unit = {
      val root = ref.get()
      val index = new Index(root, MIN, MAX)

      val n = rand.nextInt(1, 1000)

      var list = Seq.empty[(Bytes, Bytes)]

      for(i<-0 until n){
        val e = RandomStringUtils.randomAlphanumeric(rand.nextInt(1, 6)).getBytes()
        list = list :+ e -> e
      }

      if(index.insert(list)._1 && ref.compareAndSet(root, index.ctx.root) && cache.save(index.ctx)){
        data = data ++ list
      }
    }

    def remove(): Unit = {
      if(data.isEmpty) return

      val list = if(data.length > 2) scala.util.Random.shuffle(data.slice(0, rand.nextInt(1, data.length)))
        else data

      val root = ref.get()
      val index = new Index(root, MIN, MAX)

      if(index.remove(list.map(_._1))._1 && ref.compareAndSet(root, index.ctx.root) && cache.save(index.ctx)){
        data = data.filterNot{case (k, _) => list.exists{case (k1, _) => ord.equiv(k, k1)}}
      }
    }

    def update(): Unit = {

      val root = ref.get()
      var list = Query.inOrder(root, root)

      if(list.isEmpty) return

      list = if(list.length > 2) scala.util.Random.shuffle(list.slice(0, rand.nextInt(1, list.length)))
        else list

      list = list.map{case (k, _) => k -> RandomStringUtils.randomAlphanumeric(1, 6).getBytes()}

      val index = new Index(root, MIN, MAX)

      if(index.update(list)._1 && ref.compareAndSet(root, index.ctx.root) && cache.save(index.ctx)){
        data = data.filterNot{case (k, _) => list.exists{case (k1, _) => ord.equiv(k, k1)}}
        data = data ++ list
      }
    }

    for(i<-0 until iter){
      rand.nextInt(1, 4) match {
        case 1 => insert()
        case 2 => remove()
        case _ => update()
      }
    }

    val dsorted = data.sortBy(_._1)
    val isorted = Query.inOrder(ref.get(), ref.get())

    println(s"dsorted: ${dsorted.map{case (k, v) => new String(k) -> new String(v)}}\n")
    println(s"isorted: ${isorted.map{case (k, v) => new String(k) -> new String(v)}}\n")

    assert(isorted.equals(dsorted))
  }

}
