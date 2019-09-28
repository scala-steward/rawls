package org.broadinstitute.dsde.rawls.util

import io.opencensus.trace.{Span, Status}
import org.broadinstitute.dsde.rawls.dataaccess.slick.ReadWriteAction
import io.opencensus.scala.Tracing._
import slick.dbio.DBIO

import scala.concurrent.ExecutionContext


object OpenCensusUtils {

  def traceDBIOWithParent[T](
                              name: String,
                              parentSpan: Span,
                              failureStatus: Throwable => Status = (_: Throwable) => Status.UNKNOWN
                            )(f: Span => ReadWriteAction[T])(implicit executor: ExecutionContext): ReadWriteAction[T] =
    traceDBIOSpan(startSpanWithParent(name, parentSpan), failureStatus)(f)

  def traceDBIO[T](
                    name: String,
                    failureStatus: Throwable => Status = (_: Throwable) => Status.UNKNOWN
                  )(f: Span => ReadWriteAction[T])(implicit executor: ExecutionContext) : ReadWriteAction[T] =
    traceDBIOSpan(startSpan(name), failureStatus)(f)


  private def traceDBIOSpan[T](span: Span, failureStatus: Throwable => Status) (f: Span => ReadWriteAction[T])(implicit executor: ExecutionContext): ReadWriteAction[T] = {

    f(span).cleanUp { t =>
      endSpan(span, Status.OK)
      DBIO.successful(0)
    }

  }

}
