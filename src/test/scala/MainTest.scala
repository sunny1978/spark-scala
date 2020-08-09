import org.scalatest.FunSuite


class MainTest extends FunSuite {
  test("Main.message") {
    assert(Main.message(10) === 11)
  }
}
