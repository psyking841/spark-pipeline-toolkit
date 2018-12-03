lazy val BatchPipelineToolkit = (project in file("./BatchPipelineToolkit")).
  settings(Settings.commonSettings: _*).
  settings(
    Settings.assemblySettings,
    libraryDependencies ++= Settings.commonDependencies ++ Settings.batchDependencies,
    resolvers ++= Settings.resolvers
  )

lazy val WordCountDemo = (project in file("./WordCountDemo")).
  settings(Settings.commonSettings: _*).
  settings(
    Settings.assemblySettings,
    libraryDependencies ++= Settings.demoDependencies
  ).dependsOn(BatchPipelineToolkit)
