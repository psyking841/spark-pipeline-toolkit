
lazy val SparkPipelineToolkit = project.in(file("."))
  .aggregate(BatchPipelineToolkit, WordCountDemo).settings(Settings.commonSettings)

lazy val BatchPipelineToolkit = project
  .settings(
    Settings.commonSettings,
    Settings.assemblySettings,
    libraryDependencies ++= Settings.batchDependencies,
    resolvers ++= Settings.resolvers
  )

lazy val WordCountDemo = project
  .settings(
    Settings.commonSettings,
    Settings.assemblySettings,
    libraryDependencies ++= Settings.batchDependencies,
    resolvers ++= Settings.resolvers
  )
