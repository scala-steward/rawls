package org.broadinstitute.dsde.rawls.expressions.parser.antlr

import org.broadinstitute.dsde.rawls.expressions.parser.antlr.TerraExpressionParser.EntityLookupContext

import scala.collection.JavaConverters._

case class ParsedDataRepoExpression(relationships: List[String], columnName: String, expression: String, tableAlias: String) {
  val qualifiedColumnName = s"$tableAlias.$columnName"
}

class DataRepoEvaluateToAttributeVisitor(rootTableAlias: String) extends TerraExpressionBaseVisitor[Seq[ParsedDataRepoExpression]] {
  override def defaultResult(): Seq[ParsedDataRepoExpression] = Seq.empty

  override def aggregateResult(aggregate: Seq[ParsedDataRepoExpression], nextResult: Seq[ParsedDataRepoExpression]): Seq[ParsedDataRepoExpression] = {
    aggregate ++ nextResult
  }

  override def visitEntityLookup(ctx: EntityLookupContext): Seq[ParsedDataRepoExpression] = {
    val relations = ctx.relation().asScala.toList

    val tableAlias = if (relations.isEmpty) {
      rootTableAlias
    } else {
      relations.last.getText
    }

    Seq(ParsedDataRepoExpression(
      relations.map(_.attributeName().getText),
      ctx.attributeName().getText.toLowerCase,
      ctx.getText,
      tableAlias
    ))
  }
}

