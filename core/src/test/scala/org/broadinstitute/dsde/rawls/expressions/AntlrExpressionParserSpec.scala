import org.broadinstitute.dsde.rawls.expressions.parser.ExtendedJSONParser
import org.broadinstitute.dsde.rawls.expressions.{ExpressionFixture, ExpressionParser}
import org.scalatest.FlatSpec

class AntlrExpressionParserSpec extends FlatSpec with ExpressionFixture {

  // need to adjust parser for "workspace.library:cohort"

  it should "be backwards compatible" in {

    val result = parseableInputExpressionsWithNoRoot.map(x => (x, ExpressionParser.antlrParser(x))).map(x => (x._1, x._2.value()))

    println(result)
  }

  it should "be backwards compatible for parseableInputExpressionsWithRoot" in {

    val result = parseableInputExpressionsWithRoot.map(x => (x, ExpressionParser.antlrParser(x))).map(x => (x._1, x._2.value()))

    println(result)
  }

  it should "barf on invalid things" in {

//    import org.antlr.v4.runtime.RecognitionException

    unparseableInputExpressions.map {
      x => (x, ExpressionParser.antlrParser(x))
    } map { x: (String, ExtendedJSONParser) =>
      (x._1, x._2.value())
    }

  }
}
