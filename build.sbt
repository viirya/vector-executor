name         := "vector-executor"
version      := "0.1"
scalaVersion := "2.12.14"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8", "-feature")

// Use local maven repo to resolve arrow 6.0.0-SNAPSHOT.
resolvers += Resolver.mavenLocal

libraryDependencies ++= {
  Seq(
    "org.apache.spark"              %%  "spark-core"                % "3.2.0",
    "org.apache.spark"              %%  "spark-sql"                 % "3.2.0",
    "org.apache.arrow"              %  "arrow-vector"               % "6.0.0-SNAPSHOT",
    "org.apache.arrow"              %  "arrow-memory-core"          % "6.0.0-SNAPSHOT",
    "org.apache.arrow"              %  "arrow-memory-netty"         % "6.0.0-SNAPSHOT",
    "org.apache.arrow"              %  "arrow-ffi"                  % "6.0.0-SNAPSHOT"
  )
}

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "ECLIPSEF.RSA" ) => MergeStrategy.discard
  case PathList("META-INF", "mailcap" ) => MergeStrategy.discard
  case PathList("module-info.class" ) => MergeStrategy.discard
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.discard
  case PathList("netty", "util", "concurrent", ps @ _* ) => MergeStrategy.first
  case PathList("io", "netty", ps @ _*) => MergeStrategy.first
  case PathList("com", "esotericsoftware", "minlog", ps @ _*) if ps.startsWith("Log") => MergeStrategy.discard
  case PathList("plugin.properties" ) => MergeStrategy.discard
  case PathList("META-INF", ps @ _* ) => MergeStrategy.discard
  case PathList("javax", "activation", ps @ _* ) => MergeStrategy.first
  case PathList("org", "apache", "commons", ps @ _* ) => MergeStrategy.first
  case PathList("org", "apache", "hadoop", "yarn", ps @ _* ) => MergeStrategy.first
  case PathList("git.properties") => MergeStrategy.discard
  case PathList("javax", "annotation", ps @ _* ) => MergeStrategy.first
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)    
}
