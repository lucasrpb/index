package index

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{Outcome, Succeeded, TestSuite, TestSuiteMixin}

trait Retriable extends AnyFlatSpec {

  val times: Int

  override def withFixture(test: NoArgTest) = {
    withFixture(test, 0)
  }

  def withFixture(test: NoArgTest, count: Int): Outcome = {
    val outcome = super.withFixture(test)
    outcome match {
      case Succeeded => if (count + 1 == times) outcome else withFixture(test, count + 1)
      case other => other
    }
  }

}
