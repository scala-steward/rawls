package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.EntityLookupContext

import scala.collection.JavaConverters._

case class ParsedDataRepoExpression(relations: List[String], attributeName: String, expression: String)

class DataRepoEvaluateToAttributeVisitor() extends TerraExpressionBaseVisitor[Seq[ParsedDataRepoExpression]] {
  override def defaultResult(): Seq[ParsedDataRepoExpression] = Seq.empty

  override def aggregateResult(aggregate: Seq[ParsedDataRepoExpression], nextResult: Seq[ParsedDataRepoExpression]): Seq[ParsedDataRepoExpression] = {
    aggregate ++ nextResult
  }

  override def visitEntityLookup(ctx: EntityLookupContext): Seq[ParsedDataRepoExpression] = {
    Seq(ParsedDataRepoExpression(
      ctx.relation().asScala.toList.map(_.attributeName().getText),
      ctx.attributeName().getText.toLowerCase,
      ctx.getText
    ))
  }
}

