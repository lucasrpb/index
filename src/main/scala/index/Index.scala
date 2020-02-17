package index

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class Index(val ROOT: Option[String],
            val SIZE: Int,
            val TUPLE_SIZE: Int)(implicit val ord: Ordering[Bytes], val ec: ExecutionContext, cache: Cache){

  val LEAF_MIN_LENGTH = (SIZE/TUPLE_SIZE)/2

  val META_TUPLE_SIZE = TUPLE_SIZE + 36
  val META_MIN_LENGTH = (SIZE/META_TUPLE_SIZE)/2

  implicit val ctx = new Context(ROOT)

  def find(k: Bytes, start: Option[String]): Future[Option[Leaf]] = {
    start match {
      case None => Future.successful(None)
      case Some(id) => ctx.get(id).flatMap {
        case None => Future.successful(None)
        case Some(block) => block match {
          case leaf: Leaf => Future.successful(Some(leaf))
          case meta: Meta =>

            val len = meta.length
            val pointers = meta.pointers

            for(i<-0 until len){
              val (_, c) = pointers(i)
              ctx.parents += c -> (Some(meta.id), i)
            }

            find(k, meta.findPath(k))
        }
      }
    }
  }

  def find(k: Bytes): Future[Option[Leaf]] = {
    if(ctx.root.isDefined){
      ctx.parents += ctx.root.get -> (None, 0)
    }

    find(k, ctx.root)
  }

  def fixRoot(p: Block): Boolean = {
    p match {
      case p: Meta =>

        if(p.length == 1){
          val c = p.pointers(0)._2
          ctx.root = Some(c)
          ctx.parents += c -> (None, 0)
          true
        } else {
          ctx.root = Some(p.id)
          ctx.parents += p.id -> (None, 0)
          true
        }

      case p: Leaf =>
        ctx.root = Some(p.id)
        ctx.parents += p.id -> (None, 0)
        true
    }
  }

  def recursiveCopy(b: Block): Future[Boolean] = {
    val (pid, pos) = ctx.parents(b.id)

    pid match {
      case None => Future.successful(fixRoot(b))
      case Some(pid) => ctx.getMeta(pid).flatMap {
        case None => Future.successful(false)
        case Some(p) =>
          val parent = p.copy()

          parent.setPointer(Seq(Tuple3(b.last, b.id, pos)))
          recursiveCopy(parent)
      }
    }
  }

  def insertEmpty(data: Seq[Tuple]): Future[(Boolean, Int)] = {
    println(s"tree is empty ! Creating first leaf...")

    val leaf = new Leaf(UUID.randomUUID.toString, TUPLE_SIZE, LEAF_MIN_LENGTH, SIZE)

    ctx.blocks += leaf.id -> leaf
    ctx.parents += leaf.id -> (None, 0)

    val (ok, n) = leaf.insert(data)

    if(!ok) return Future.successful(false -> 0)

    recursiveCopy(leaf).map(_ -> n)
  }

  def insertParent(left: Meta, prev: Block): Future[Boolean] = {
    if(left.isFull()){

      println(s"parent is full! Splitting...")

      val right = left.split()

      if(ord.gt(prev.last, left.last)){
        right.insert(Seq(prev.last -> prev.id))
      } else {
        left.insert(Seq(prev.last -> prev.id))
      }

      return handleParent(left, right)
    }

    println(s"parent not full ! Inserting...")

    left.insert(Seq(prev.last -> prev.id))._1

    recursiveCopy(left)
  }

  def handleParent(left: Block, right: Block): Future[Boolean] = {
    val (pid, pos) = ctx.parents(left.id)

    pid match {
      case None =>

        println(s"new level...")

        val r = new Meta(UUID.randomUUID.toString, META_TUPLE_SIZE, META_MIN_LENGTH, SIZE)

        ctx.blocks += r.id -> r
        ctx.parents += r.id -> (None, 0)

        r.insert(Seq(
          left.last -> left.id,
          right.last -> right.id
        ))

        recursiveCopy(r)

      case Some(pid) => ctx.getMeta(pid).flatMap {
        case None => Future.successful(false)
        case Some(p) =>
          val parent = p.copy()
          parent.setPointer(Seq((left.last, left.id, pos)))
          insertParent(parent, right)
      }
    }
  }

  def insertLeaf(leaf: Leaf, data: Seq[Tuple]): Future[(Boolean, Int)] = {

    val left = leaf.copy()

    if(left.isFull()){

      println(s"leaf full ! Splitting...")

      val right = left.split()
      return handleParent(left, right).map(_ -> 0)
    }

    println(s"leaf not full! Inserting...")

    val (ok, n) = left.insert(data)

    if(!ok) return Future.successful(false -> 0)

    recursiveCopy(left).map(_ -> n)
  }

  def insert(data: Seq[Tuple]): Future[(Boolean, Int)] = {

    if(data.map{case (k, v) => k.length + v.length}.exists(_ > TUPLE_SIZE)){
      println(s"MAX TUPLE SIZE :(")
      return Future.successful(false -> 0)
    }

    val sorted = data.sortBy(_._1)

    if(sorted.exists{case (k, _) => data.count{case (k1, _) => ord.equiv(k, k1)} > 1}){
      return Future.successful(false -> 0)
    }

    val len = sorted.length
    var pos = 0

    def insert(): Future[(Boolean, Int)] = {
      if(pos == len) return Future.successful(true -> len)

      var list = sorted.slice(pos, len)
      val (k, _) = list(0)

      find(k).flatMap {
        case None => insertEmpty(list)
        case Some(leaf) =>

          val idx = list.indexWhere {case (k, _) => ord.gt(k, leaf.last)}
          if(idx > 0) list = list.slice(0, idx)

          insertLeaf(leaf, list)
      }.flatMap { case (ok, n) =>
        if(!ok) {
          Future.successful(false -> 0)
        } else {
          pos += n
          insert()
        }
      }
    }

    insert()
  }

  def merge(left: Block, lpos: Int, right: Block, rpos: Int, parent: Meta)(side: String): Future[Boolean] = {

    left.merge(right)

    parent.setPointer(Seq((left.last, left.id, lpos)))
    parent.removeAt(rpos)

    if(parent.hasMinimum()){

      println(s"${Console.YELLOW_B}data merging from $side ...${Console.RESET}\n")

      return recursiveCopy(parent)
    }

    val (gopt, gpos) = ctx.parents(parent.id)

    if(gopt.isEmpty){

      if(parent.isEmpty()){

        println(s"one level less... merged: ${left}\n")

        ctx.parents += left.id -> (None, 0)
        ctx.root = Some(left.id)

        return Future.successful(true)
      }

      return recursiveCopy(parent)
    }

    ctx.getMeta(gopt.get).flatMap {
      case None => Future.successful(false)
      case Some(gparent) => borrow(parent, gparent.copy(), gpos)
    }
  }

  def borrowFromRight(target: Block, left: Option[Block], ropt: Option[String], parent: Meta, pos: Int): Future[Boolean] = {
    if(ropt.isDefined){
      return ctx.get(ropt.get).flatMap {
        case None => Future.successful(false)
        case Some(rnode) =>
          if(rnode.canBorrowTo(target)){
            val right = rnode.copy()

            right.borrowRightTo(target)

            parent.setPointer(Seq(
              Tuple3(target.last, target.id, pos),
              Tuple3(right.last, right.id, pos + 1)
            ))

            println(s"${Console.BLUE_B}data borrowing from right...${Console.RESET}\n")

            recursiveCopy(parent)
          } else {
            merge(target, pos, rnode.copy(), pos + 1, parent)("right")
          }
      }
    }

    merge(left.get, pos - 1, target, pos, parent)("left")
  }

  def borrowFromLeft(target: Block, lopt: Option[String], ropt: Option[String], parent: Meta, pos: Int): Future[Boolean] = {
    if(lopt.isDefined){
      return ctx.get(lopt.get).flatMap {
        case None => Future.successful(false)
        case Some(lnode) =>

          if(lnode.canBorrowTo(target)){
            val left = lnode.copy()

            left.borrowLeftTo(target)

            parent.setPointer(Seq(
              Tuple3(left.last, left.id, pos - 1),
              Tuple3(target.last, target.id, pos)
            ))

            println(s"${Console.BLUE_B}data borrowing from left...${Console.RESET}\n")

            recursiveCopy(parent)
          } else {
            borrowFromRight(target, Some(lnode.copy()), ropt, parent, pos)
          }
      }
    }

    borrowFromRight(target, None, ropt, parent, pos)
  }

  def borrow(target: Block, parent: Meta, pos: Int): Future[Boolean] = {

    val lopt = parent.left(pos)
    val ropt = parent.right(pos)

    if(lopt.isEmpty && ropt.isEmpty){

      println(s"no data siblings...")

      if(target.isEmpty()){
        ctx.root = None
        return Future.successful(true)
      }

      ctx.parents += target.id -> (None, 0)
      ctx.root = Some(target.id)

      return Future.successful(true)
    }

    borrowFromLeft(target, lopt, ropt, parent, pos)
  }

  def removeFromLeaf(leaf: Leaf, keys: Seq[Bytes]): Future[(Boolean, Int)] = {
    val target = leaf.copy()

    val (ok, n) = target.remove(keys)

    if(!ok) return Future.successful(false -> 0)

    if(target.hasMinimum()){
      println(s"removal from leaf...\n")
      return recursiveCopy(target).map(_ -> n)
    }

    val (pid, pos) = ctx.parents(target.id)

    if(pid.isEmpty){

      if(target.isEmpty()){
        ctx.root = None
        return Future.successful(true -> n)
      }

      return recursiveCopy(target).map(_ -> n)
    }

    ctx.getMeta(pid.get).flatMap {
      case None => Future.successful(false -> 0)
      case Some(parent) => borrow(target, parent.copy(), pos).map(_ -> n)
    }
  }

  def remove(keys: Seq[Bytes]): Future[(Boolean, Int)] = {
    val sorted = keys.sorted

    if(sorted.exists{k => keys.count{k1 => ord.equiv(k, k1)} > 1}){
      return Future.successful(false -> 0)
    }

    val len = sorted.length
    var pos = 0

    def remove(): Future[(Boolean, Int)] = {
      if(pos == len) return Future.successful(true -> len)

      var list = sorted.slice(pos, len)
      val k = list(0)

      find(k).flatMap {
        case None => Future.successful(false -> 0)
        case Some(leaf) =>

          val idx = list.indexWhere {k => ord.gt(k, leaf.last)}
          list = if(idx > 0) list.slice(0, idx) else list

          removeFromLeaf(leaf, list)
      }.flatMap { case (ok, n) =>
        if(!ok){
          Future.successful(false -> 0)
        } else {
          pos += n
          remove()
        }
      }
    }

    remove()
  }

  def updateLeaf(leaf: Leaf, data: Seq[Tuple]): Future[(Boolean, Int)] = {
    val left = leaf.copy()

    if(left.isFull()){
      val right = left.split()
      return handleParent(left, right).map(_ -> 0)
    }

    val (ok, n) = left.update(data)

    if(!ok) return Future.successful(false -> 0)

    recursiveCopy(left).map(_ -> n)
  }

  def update(data: Seq[Tuple]): Future[(Boolean, Int)] = {

    if(data.map{case (k, v) => k.length + v.length}.exists(_ > TUPLE_SIZE)){
      println(s"UPDATE MAX TUPLE SIZE :(")
      return Future.successful(false -> 0)
    }

    val sorted = data.sortBy(_._1)

    if(sorted.exists{case (k, _) => data.count{case (k1, _) => ord.equiv(k, k1)} > 1}){
      return Future.successful(false -> 0)
    }

    val len = sorted.length
    var pos = 0

    def update(): Future[(Boolean, Int)] = {
      if(pos == len) return Future.successful(true -> len)

      var list = sorted.slice(pos, len)
      val (k, _) = list(0)

      find(k).flatMap {
        case None => Future.successful(false -> 0)
        case Some(leaf) =>

          val idx = list.indexWhere {case (k, _) => ord.gt(k, leaf.last)}
          if(idx > 0) list = list.slice(0, idx)

          updateLeaf(leaf, list)
      }.flatMap { case (ok, n) =>
        if(!ok){
          Future.successful(false -> 0)
        } else {
          pos += n
          update()
        }
      }
    }

    update()
  }

}
