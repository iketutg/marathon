#!/usr/bin/env amm

import $ivy.`org.eclipse.jgit:org.eclipse.jgit:4.8.0.201706111038-r`

import ammonite.ops._
import ammonite.ops.ImplicitWd._
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.internal.storage.file.FileRepository
import org.eclipse.jgit.merge.MergeStrategy
import org.eclipse.jgit.transport.{ RefSpec, URIish }
import upickle._

def update(key: String, m: (upickle.Js.Value) => upickle.Js.Value)(obj: upickle.Js.Obj): upickle.Js.Obj = {
  val updatedValues: Seq[(String, upickle.Js.Value)] = obj.value.map { case (k: String, v: upickle.Js.Value) =>
    if(k == key) k -> m(v) else k -> v
  }
  upickle.Js.Obj(updatedValues:_*)
}

@main
def upgrade(url: String, sha1: String): Unit = {
  val buildinfoPath = pwd / 'dcos / 'packages/ 'marathon / "buildinfo.json"
  val buildinfoData = read(buildinfoPath)

  val buildinfo: upickle.Js.Obj = upickle.json.read(buildinfoData).asInstanceOf[upickle.Js.Obj]

  val updated = update("single_source", {
    case o: upickle.Js.Obj =>
      val t = update( "url", { case e: upickle.Js.Str => e.copy(value = url) } )(o)
      update( "sha1", { case e: upickle.Js.Str => e.copy(value = sha1)} )(t)
    case i => i }
  )(buildinfo)

  write.over(buildinfoPath, upickle.json.write(updated))
  write.append(buildinfoPath, "\n")
}

@main
def checkout(): Unit = {
  val dcosRepoPath = pwd / 'dcos

  rm! dcosRepoPath

  // TODO: For the implementation we'll use git@github.com:mesosphere/dcos.git
  val git = Git.cloneRepository()
    .setURI("git@github.com:jeschkies/dcos.git")
    .setBranch("marathon/latest")
    .setDirectory(dcosRepoPath.toIO)
    .call()

  // Update with latest DC/OS
  val remoteAddCmd = git.remoteAdd()
  remoteAddCmd.setName("dcos")
  remoteAddCmd.setUri(new URIish("https://github.com/dcos/dcos.git"))
  remoteAddCmd.call()

  git.pull()
    .setRemote("dcos")
    .setRemoteBranchName("master")
    .setStrategy(MergeStrategy.THEIRS)
    .call()
}

@main
def commitAndPush(version: String = "unknown"): Unit = {
  val dcosRepoPath = pwd / 'dcos
  val buildinfoPath = dcosRepoPath / 'packages/ 'marathon / "buildinfo.json"

  val git = new Git(new FileRepository( (dcosRepoPath / ".git").toIO ))

  git.add().addFilepattern("packages/marathon").call()
  val commit = git.commit().setMessage(s"Upgrade Marathon version to $version").call()

  git.push()
    .setRefSpecs(new RefSpec(s"${commit.getId.getName}:refs/heads/marathon/latest"))
    .call()
}

@main
def all(): Unit = {
  checkout()
  upgrade(
    "https://s3.amazonaws.com/downloads.mesosphere.io/marathon/snapshots/marathon-1.5.0-SNAPSHOT-638-g52d5f15.tgz",
    "54178036606e94c2835b7d4a64018f984c2c6d3a"
  )
  commitAndPush("1.5.0-SNAPSHOT-638-g52d5f15")
}
