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

class MainSpec extends Retriable {

  override val times = 100

  "index data " must "be equal to list data" in {

    val rand = ThreadLocalRandom.current()

    implicit val ord = new Ordering[Bytes] {
      val c = UnsignedBytes.lexicographicalComparator()
      override def compare(x: Bytes, y: Bytes): Int = c.compare(x, y)
    }

    val ref = new AtomicReference[Option[String]](None)
    implicit val cache = new MemoryCache()
    var data = Seq.empty[Tuple]

    def insert(): Future[(Boolean, Seq[Tuple])] = {
      val root = ref.get()
      val index = new Index(root, 2, 4)

      val n = rand.nextInt(1, 1000)

      var list = Seq.empty[(Bytes, Bytes)]

      for(i<-0 until n){
        val e = RandomStringUtils.randomAlphanumeric(rand.nextInt(1, 10)).getBytes()

        //if(!list.exists{case (k, _) => ord.equiv(k, e)}){
          list = list :+ e -> e
        //}
      }

      if(index.insert(list)._1 && ref.compareAndSet(root, index.ctx.root) && cache.save(index.ctx)){
        return Future.successful(true -> list)
      }

      Future.successful(false -> Seq.empty[Tuple])
    }

    var tasks = Seq.empty[Future[(Boolean, Seq[Tuple])]]
    val iterations = 10//rand.nextInt(4, 100)

    for(i<-0 until iterations){
      tasks = tasks :+ insert()
    }

    val results = Await.result(Future.sequence(tasks), 3 seconds)

    val successes = results.filter(_._1 == true)

    println(s"successes: ${successes.length}/${iterations}\n")

    successes.foreach { case (_, list) =>
      data = data ++ list
    }

    val slist = data.sortBy(_._1).map(_._1)
    val ilist = Query.inOrder(ref.get(), ref.get()).map(_._1)

    println(s"list: ${slist.map{new String(_)}}\n")
    println(s"ilist: ${ilist.map{new String(_)}}\n")

    assert(ilist.equals(slist))

    //Query.prettyPrint(ref.get())
  }

}
