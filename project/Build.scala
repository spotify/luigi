import sbt._

object ScaldingBuild extends Build {
  lazy val root = Project("root", file("."))
                    .dependsOn(RootProject(uri("git://github.com/twitter/algebird.git#master")))
                    .dependsOn(RootProject(uri("git://github.com/twitter/chill.git#master")))
}
