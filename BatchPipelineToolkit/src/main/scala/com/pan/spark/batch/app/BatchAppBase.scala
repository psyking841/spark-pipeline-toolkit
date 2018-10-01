package com.pan.spark.batch.app

/**
  * Base class for all Spark batch applications to extend.
  */
class BatchAppBase extends BatchAppController with App{
  /**
    * Execute the batch transformation
    * @param batchprocessorFunction
    */
  def run(batchprocessorFunction: => Unit): Unit = {
    if(defaultSettings.defaultConfigs.hasPath("dryRun")) getCommand
    else batchprocessorFunction
  }
}
