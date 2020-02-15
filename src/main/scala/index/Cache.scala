package index

import scala.concurrent.Future

trait Cache {

  def get(id: String): Future[Option[Block]]
  def save(ctx: Context): Future[Boolean]

}
