package com.span.spark.batch.app

/**
  * Base class for all Spark batch applications to extend.
  */
class BatchAppBase extends BatchAppController with App{
  /**
    * Execute the batch transformation
    * @param batchprocessorFunction
    */
  def run(batchprocessorFunction: => Unit): Unit = {
    if (defaultSettings.defaultConfigs.hasPath("dryRun")) {
      logger.info("In dryrun mode, printing the command line: ")
      logger.info(getCommand())
    } else batchprocessorFunction
  }
}
