
lazy val BatchPipelineToolkit = project
  .settings(
    Settings.commonSettings,
    Settings.assemblySettings,
    libraryDependencies ++= (Settings.commonDependencies ++ Settings.batchDependencies),
    resolvers ++= Settings.resolvers
  )

lazy val WordCountDemo = project
  .settings(
    Settings.commonSettings,
    Settings.assemblySettings,
    libraryDependencies ++= (Settings.commonDependencies ++ Settings.demoDependencies),
    resolvers ++= Settings.resolvers
  )
