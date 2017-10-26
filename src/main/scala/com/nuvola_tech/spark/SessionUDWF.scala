package com.nuvola_tech.spark

import java.util.UUID

import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.catalyst.expressions.{Add, AggregateWindowFunction, AttributeReference, Expression, If, IsNotNull, LessThanOrEqual, Literal, ScalaUDF, Subtract}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String



object MyUDWF {
  val defaultMaxSessionLengthms = 3600 * 1000
  case class SessionUDWF(timestamp:Expression, session:Expression,
                         sessionWindow:Expression = Literal(defaultMaxSessionLengthms)) extends AggregateWindowFunction {
    self: Product =>

    override def children: Seq[Expression] = Seq(timestamp, session)
    override def dataType: DataType = StringType

    protected val zero = Literal( 0L )
    protected val nullString = Literal(null:String)

    protected val curentSession = AttributeReference("currentSession", StringType, nullable = true)()
    protected val previousTs =    AttributeReference("lastTs", LongType, nullable = false)()

    override val aggBufferAttributes: Seq[AttributeReference] =  curentSession  :: previousTs :: Nil

    protected val assignSession =  If(LessThanOrEqual(Subtract(timestamp, aggBufferAttributes(1)), sessionWindow),
      aggBufferAttributes(0), // if
      ScalaUDF( createNewSession, StringType, children = Nil))

    override val initialValues: Seq[Expression] =  nullString :: zero :: Nil
    override val updateExpressions: Seq[Expression] =
      If(IsNotNull(session), session, assignSession) ::
        timestamp ::
        Nil

    override val evaluateExpression: Expression = aggBufferAttributes(0)
    override def prettyName: String = "makeSession"
  }

  protected val  createNewSession = () => org.apache.spark.unsafe.types.UTF8String.fromString(UUID.randomUUID().toString)

  def calculateSession(ts:Column,sess:Column): Column = withExpr { SessionUDWF(ts.expr,sess.expr, Literal(defaultMaxSessionLengthms)) }
  def calculateSession(ts:Column,sess:Column, sessionWindow:Column): Column = withExpr { SessionUDWF(ts.expr,sess.expr, sessionWindow.expr) }

  private def withExpr(expr: Expression): Column = new Column(expr)
}
