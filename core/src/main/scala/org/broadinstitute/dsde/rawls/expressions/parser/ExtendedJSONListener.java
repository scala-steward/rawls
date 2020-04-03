// Generated from /Users/anichols/OneDrive/Broad-Institute/Bugs/WA-163/resolver2/ExtendedJSON.g4 by ANTLR 4.8
package org.broadinstitute.dsde.rawls.expressions.parser;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link ExtendedJSONParser}.
 */
public interface ExtendedJSONListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link ExtendedJSONParser#obj}.
	 * @param ctx the parse tree
	 */
	void enterObj(ExtendedJSONParser.ObjContext ctx);
	/**
	 * Exit a parse tree produced by {@link ExtendedJSONParser#obj}.
	 * @param ctx the parse tree
	 */
	void exitObj(ExtendedJSONParser.ObjContext ctx);
	/**
	 * Enter a parse tree produced by {@link ExtendedJSONParser#pair}.
	 * @param ctx the parse tree
	 */
	void enterPair(ExtendedJSONParser.PairContext ctx);
	/**
	 * Exit a parse tree produced by {@link ExtendedJSONParser#pair}.
	 * @param ctx the parse tree
	 */
	void exitPair(ExtendedJSONParser.PairContext ctx);
	/**
	 * Enter a parse tree produced by {@link ExtendedJSONParser#arr}.
	 * @param ctx the parse tree
	 */
	void enterArr(ExtendedJSONParser.ArrContext ctx);
	/**
	 * Exit a parse tree produced by {@link ExtendedJSONParser#arr}.
	 * @param ctx the parse tree
	 */
	void exitArr(ExtendedJSONParser.ArrContext ctx);
	/**
	 * Enter a parse tree produced by {@link ExtendedJSONParser#lookup}.
	 * @param ctx the parse tree
	 */
	void enterLookup(ExtendedJSONParser.LookupContext ctx);
	/**
	 * Exit a parse tree produced by {@link ExtendedJSONParser#lookup}.
	 * @param ctx the parse tree
	 */
	void exitLookup(ExtendedJSONParser.LookupContext ctx);
	/**
	 * Enter a parse tree produced by {@link ExtendedJSONParser#value}.
	 * @param ctx the parse tree
	 */
	void enterValue(ExtendedJSONParser.ValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link ExtendedJSONParser#value}.
	 * @param ctx the parse tree
	 */
	void exitValue(ExtendedJSONParser.ValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link ExtendedJSONParser#literal}.
	 * @param ctx the parse tree
	 */
	void enterLiteral(ExtendedJSONParser.LiteralContext ctx);
	/**
	 * Exit a parse tree produced by {@link ExtendedJSONParser#literal}.
	 * @param ctx the parse tree
	 */
	void exitLiteral(ExtendedJSONParser.LiteralContext ctx);
}