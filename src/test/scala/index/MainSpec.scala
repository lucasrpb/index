package index

import java.util.concurrent.ThreadLocalRandom
import org.scalatest.flatspec.AnyFlatSpec

class MainSpec extends AnyFlatSpec {

  "index data " must "be equal to list data" in {

    val rand = ThreadLocalRandom.current()
    val n = 100

    implicit val ord = new Ordering[Int] {
      override def compare(x: Int, y: Int): Int = x - y
    }

    val leaf = new Leaf[Int, Int](50, 100)

    var list = Seq.empty[(Int, Int)]

    for(i<-0 until n){
      val e = rand.nextInt(0, 1000)

      if(!list.exists{case (k, _) => ord.equiv(k, e)}){
        list = list :+ e -> e
      }
    }

    leaf.insert(list)

    val slist = list.sortBy(_._1)
    val sleaf = leaf.inOrder()

    println(s"list: ${slist}\n")
    println(s"leaf: ${sleaf}\n")

    assert(sleaf.equals(slist))
  }

}
