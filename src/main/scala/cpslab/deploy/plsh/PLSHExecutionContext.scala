package cpslab.deploy.plsh

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

private[plsh] object PLSHExecutionContext {
  implicit lazy val executorService : ExecutionContext = {
    val executorService = Executors.newCachedThreadPool()
    ExecutionContext.fromExecutorService(executorService)
  }
}
