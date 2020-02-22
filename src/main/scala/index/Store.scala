package index

import scala.concurrent.Future

trait Store {

  def get[T <: Block](id: String): Future[Option[T]]
  def save(ctx: Context): Future[Boolean]

}
