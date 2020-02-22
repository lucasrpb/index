package index

import com.datastax.driver.core.{Cluster, Session}
import scala.concurrent.{ExecutionContext, Future}

class CQLStore(val NR_ENTRIES: Int)
              (implicit val ec: ExecutionContext, val ord: Ordering[Bytes], serializer: Serializer) extends Store {

  val MAX = NR_ENTRIES
  val MIN = MAX/2

  val cluster = Cluster.builder().addContactPoints("127.0.0.1").withPort(9042).build()
  implicit val session = cluster.connect("index")

  println(s"TRUNCATED: ${session.execute("TRUNCATE blocks;").wasApplied()}\n")

  val INSERT = session.prepare("insert into blocks(id, bin, leaf) values (?, ?, ?);")
  val SELECT = session.prepare("select * from blocks where id=?;")

  def read(id: String): Future[Option[Block]] = {
    session.executeAsync(SELECT.bind.setString(0, id)).map { rs =>
      val one = rs.one()

      if(one != null){
        Some(one.getBool("leaf") match {
          case true => serializer.deserializeLeaf(one.getBytes("bin"))
          case false => serializer.deserializeMeta(one.getBytes("bin"))
        })
      } else {
        None
      }
    }
  }

  override def get[T <: Block](id: String): Future[Option[T]] = {
    read(id).map(_.map(_.asInstanceOf[T]))
  }

  override def save(ctx: Context): Future[Boolean] = {
    /*val batchStmt = new BatchStatement()
    val boundStatement = new BoundStatement(insert)
    println(s"SAVING ${ctx.blocks.map(_._2.size).sum} ids: ${ctx.blocks.map(_._1).toSeq}\n")
    ctx.blocks.map { case (id, b) =>
      var isLeaf = false
      val bin = b match {
        case leaf: Leaf =>
          isLeaf = true
          serialize(leaf)
        case meta: Meta => serialize(meta)
      }
      batchStmt.add(boundStatement.setString(0, "local").setString(1, id).setBytes(2, bin).setBool(3, isLeaf))
    }
    println(s"batch size ${batchStmt.size()}")
    session.executeAsync(batchStmt).map(_.wasApplied())*/

    val tasks = ctx.blocks.map { case (id, b) =>
      val insert = INSERT.bind()

      var isLeaf = false
      val bin = b match {
        case leaf: Leaf =>
          isLeaf = true
          serializer.serialize(leaf)

        case meta: Meta => serializer.serialize(meta)
      }

      insert.setString(0, id).setBytes(1, bin).setBool(2, isLeaf)

      session.executeAsync(insert).map(_.wasApplied())
    }

    Future.sequence(tasks).map(!_.exists(_ == false))
  }

}
