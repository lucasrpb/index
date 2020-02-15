package index

import java.util.UUID

class Index(val ROOT: Option[String],
            val SIZE: Int,
            val TUPLE_SIZE: Int)(implicit val ord: Ordering[Bytes], cache: Cache){

  val LEAF_MIN_LENGTH = (SIZE/TUPLE_SIZE)/2

  val META_TUPLE_SIZE = TUPLE_SIZE + 36
  val META_MIN_LENGTH = (SIZE/META_TUPLE_SIZE)/2

  implicit val ctx = new Context(ROOT)

  def find(k: Bytes, start: Option[String]): Option[Leaf] = {
    start match {
      case None => None
      case Some(id) => ctx.get(id) match {
        case leaf: Leaf => Some(leaf)
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

  def find(k: Bytes): Option[Leaf] = {
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

  def recursiveCopy(p: Block): Boolean = {
    val (pid, pos) = ctx.parents(p.id)

    pid match {
      case None => fixRoot(p)
      case Some(pid) =>
        val parent = ctx.getMeta(pid).copy()
        parent.setPointer(Seq(Tuple3(p.last, p.id, pos)))

        recursiveCopy(parent)
    }
  }

  def insertEmpty(data: Seq[Tuple]): (Boolean, Int) = {

    println(s"tree is empty ! Creating first leaf...")

    val leaf = new Leaf(UUID.randomUUID.toString, TUPLE_SIZE, LEAF_MIN_LENGTH, SIZE)

    ctx.blocks += leaf.id -> leaf
    ctx.parents += leaf.id -> (None, 0)

    val (ok, n) = leaf.insert(data)

    if(!ok) return false -> 0

    recursiveCopy(leaf) -> n
  }

  def insertParent(left: Meta, prev: Block): Boolean = {
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

  def handleParent(left: Block, right: Block): Boolean = {
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

      case Some(pid) =>
        val parent = ctx.getMeta(pid).copy()

        parent.setPointer(Seq((left.last, left.id, pos)))
        insertParent(parent, right)
    }
  }

  def insert(leaf: Leaf, data: Seq[Tuple]): (Boolean, Int) = {

    val left = leaf.copy()

    if(left.isFull()){

      println(s"leaf full ! Splitting...")

      val right = left.split()
      return handleParent(left, right) -> 0
    }

    println(s"leaf not full! Inserting...")

    val (ok, n) = left.insert(data)

    if(!ok) return false -> 0

    recursiveCopy(left) -> n
  }

  def insert(data: Seq[Tuple]): (Boolean, Int) = {

    if(data.map{case (k, v) => k.length + v.length}.exists(_ > TUPLE_SIZE)){
      println(s"MAX TUPLE SIZE :(")
      return false -> 0
    }

    val sorted = data.sortBy(_._1)

    if(sorted.exists{case (k, _) => data.count{case (k1, _) => ord.equiv(k, k1)} > 1}){
      return false -> 0
    }

    val size = sorted.length
    var pos = 0

    while(pos < size){

      var list = sorted.slice(pos, size)
      val (k, _) = list(0)

      val (ok, n) = find(k) match {
        case None => insertEmpty(list)
        case Some(p) =>

          val idx = list.indexWhere{case (k, _) => ord.gt(k, p.last)}
          if(idx > 0) list = list.slice(0, idx)

          insert(p, list)
      }

      if(!ok) return false -> 0

      pos += n
    }

    true -> size
  }

  def merge(left: Meta, lpos: Int, right: Meta, rpos: Int, parent: Meta)(side: String): Boolean = {

    left.merge(right)

    parent.setPointer(Seq((left.last, left.id, lpos)))
    parent.removeAt(rpos)

    if(parent.hasMinimum()){

      println(s"${Console.YELLOW}meta merging from $side ...\n${Console.RESET}")

      return recursiveCopy(parent)
    }

    val (gopt, gpos) = ctx.parents(parent.id)

    if(gopt.isEmpty){

      if(parent.isEmpty()){

        println(s"one level less...\n")

        ctx.parents += left.id -> (None, 0)
        ctx.root = Some(left.id)

        return true
      }

      return recursiveCopy(parent)
    }

    val gparent = ctx.getMeta(gopt.get).copy()

    borrow(parent, gparent, gpos)
  }

  def borrow(target: Meta, parent: Meta, pos: Int): Boolean = {

    val lpos = pos - 1
    val rpos = pos + 1

    val lopt = parent.left(pos)//.map(ctx.getMeta(_).copy())
    val ropt = parent.right(pos)//.map(ctx.getMeta(_).copy())

    if(lopt.isEmpty && ropt.isEmpty){

      println(s"no meta siblings... grandpa ${ctx.parents(parent.id)._1} parent ${parent} target ${target}\n")

      ctx.parents += target.id -> (None, 0)
      ctx.root = Some(target.id)

      return true
    }

    val lnode = lopt.map(ctx.getMeta(_))

    if(lnode.isDefined && lnode.get.canBorrowTo(target)){

      val left = lnode.get.copy()

      left.borrowLeftTo(target)

      parent.setPointer(Seq(
        Tuple3(left.last, left.id, lpos),
        Tuple3(target.last, target.id, pos)
      ))

      println(s"${Console.RED}meta borrowing from left...\n${Console.RESET}")

      return recursiveCopy(parent)
    }

    val rnode = ropt.map(ctx.getMeta(_))

    if(rnode.isDefined && rnode.get.canBorrowTo(target)){

      val right = rnode.get.copy()

      right.borrowRightTo(target)

      parent.setPointer(Seq(
        Tuple3(target.last, target.id, pos),
        Tuple3(right.last, right.id, rpos)
      ))

      println(s"${Console.RED}meta borrowing from right...\n${Console.RESET}")

      return recursiveCopy(parent)
    }

    if(lnode.isDefined){
      return merge(lnode.get.copy(), lpos, target, pos, parent)("left")
    }

    merge(target, pos, rnode.get.copy(), rpos, parent)("right")
  }

  def merge(left: Leaf, lpos: Int, right: Leaf, rpos: Int, parent: Meta)(side: String): Boolean = {

    left.merge(right)

    parent.setPointer(Seq((left.last, left.id, lpos)))
    parent.removeAt(rpos)

    if(parent.hasMinimum()){

      println(s"data merging from $side ...\n")

      return recursiveCopy(parent)
    }

    val (gopt, gpos) = ctx.parents(parent.id)

    if(gopt.isEmpty){

      if(parent.isEmpty()){

        println(s"one level less... merged: ${left}\n")

        ctx.parents += left.id -> (None, 0)
        ctx.root = Some(left.id)

        return true
      }

      return recursiveCopy(parent)
    }

    val gparent = ctx.getMeta(gopt.get).copy()

    borrow(parent, gparent, gpos)
  }

  def borrow(target: Leaf, parent: Meta, pos: Int): Boolean = {

    val lpos = pos - 1
    val rpos = pos + 1

    val lopt = parent.left(pos)//.map(ctx.getLeaf(_).copy())
    val ropt = parent.right(pos)//.map(ctx.getLeaf(_).copy())

    if(lopt.isEmpty && ropt.isEmpty){

      println(s"no data siblings...")

      if(target.isEmpty()){
        ctx.root = None
        return true
      }

      ctx.parents += target.id -> (None, 0)
      ctx.root = Some(target.id)

      return true
    }

    val lnode = lopt.map(ctx.getLeaf(_))

    if(lnode.isDefined && lnode.get.canBorrowTo(target)){

      val left = lnode.get.copy()

      left.borrowLeftTo(target)

      parent.setPointer(Seq(
        Tuple3(left.last, left.id, lpos),
        Tuple3(target.last, target.id, pos)
      ))

      println(s"data borrowing from left...\n")

      return recursiveCopy(parent)
    }

    val rnode = ropt.map(ctx.getLeaf(_))

    if(rnode.isDefined && rnode.get.canBorrowTo(target)){

      val right = rnode.get.copy()

      right.borrowRightTo(target)

      parent.setPointer(Seq(
        Tuple3(target.last, target.id, pos),
        Tuple3(right.last, right.id, rpos)
      ))

      println(s"data borrowing from right...\n")

      return recursiveCopy(parent)
    }

    if(lnode.isDefined){
      return merge(lnode.get.copy(), lpos, target, pos, parent)("left")
    }

    merge(target, pos, rnode.get.copy(), rpos, parent)("right")
  }

  def remove(leaf: Leaf, keys: Seq[Bytes]): (Boolean, Int) = {
    val target = leaf.copy()

    println(s"removing ${keys.map(new String(_))} target: ${target}\n")

    val (ok, n) = target.remove(keys)

    if(!ok) {
      println(s"so sad!")
      return false -> 0
    }

    if(target.hasMinimum()){
      println(s"removal from leaf...\n")
      return recursiveCopy(target) -> n
    }

    val (pid, pos) = ctx.parents(target.id)

    if(pid.isEmpty){

      if(target.isEmpty()){
        ctx.root = None
        return true -> n
      }

      return recursiveCopy(target) -> n
    }

    val parent = ctx.getMeta(pid.get).copy()

    borrow(target, parent, pos) -> n
  }

  def remove(keys: Seq[Bytes]): (Boolean, Int) = {
    val sorted = keys.sorted

    if(sorted.exists{k => keys.count{k1 => ord.equiv(k, k1)} > 1}){
      return false -> 0
    }

    val size = sorted.length
    var pos = 0

    while(pos < size) {

      var list = sorted.slice(pos, size)
      val k = list(0)

      val (ok, n) = find(k) match {
        case None => false -> 0
        case Some(leaf) =>

          val idx = list.indexWhere {k => ord.gt(k, leaf.last)}
          list = if(idx > 0) list.slice(0, idx) else list

          remove(leaf, list)
      }

      if(!ok) return false -> 0

      pos += n
    }

    true -> size
  }


  def update(leaf: Leaf, data: Seq[Tuple]): (Boolean, Int) = {
    val left = leaf.copy()

    if(left.isFull()){
      val right = left.split()
      return handleParent(left, right) -> 0
    }

    val (ok, n) = left.update(data)

    if(!ok) return false -> 0

    recursiveCopy(left) -> n
  }

  def update(data: Seq[Tuple]): (Boolean, Int) = {

    if(data.map{case (k, v) => k.length + v.length}.exists(_ > TUPLE_SIZE)){
      println(s"UPDATE MAX TUPLE SIZE :(")
      return false -> 0
    }

    val sorted = data.sortBy(_._1)

    if(sorted.exists{case (k, _) => data.count{case (k1, _) => ord.equiv(k, k1)} > 1}){
      return false -> 0
    }

    val size = sorted.length
    var pos = 0

    while(pos < size){

      var list = sorted.slice(pos, size)
      val (k, _) = list(0)

      val (ok, n) = find(k) match {
        case None => false -> 0
        case Some(leaf) =>

          val idx = list.indexWhere {case (k, _) => ord.gt(k, leaf.last)}
          if(idx > 0) list = list.slice(0, idx)

          update(leaf, list)
      }

      if(!ok) return false -> 0

      pos += n
    }

    true -> size
  }

}
