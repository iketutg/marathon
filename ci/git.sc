#!/usr/bin/env amm

import $ivy.`org.eclipse.jgit:org.eclipse.jgit:4.8.0.201706111038-r`

import ammonite.ops._
import ammonite.ops.ImplicitWd._
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.internal.storage.file.FileRepository
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
  val buildinfoPath = dcosRepoPath / 'packages/ 'marathon / "buildinfo.json"

  rm! dcosRepoPath

  val git = Git.cloneRepository()
    .setURI("https://github.com/mesosphere/dcos.git")
    .setDirectory(dcosRepoPath.toIO)
    .call()
}

@main
def commitAndPush(version: String = "unknown"): Unit = {
  val dcosRepoPath = pwd / 'dcos
  val buildinfoPath = dcosRepoPath / 'packages/ 'marathon / "buildinfo.json"

  val git = new Git(new FileRepository( (dcosRepoPath / ".git").toIO ))

  git.add().addFilepattern("packages/marathon").call()
  val commit = git.commit().setMessage(s"Upgrade Marathon version to $version").call()

  val cmd = git.remoteAdd()
  cmd.setName("jeschkies")
  cmd.setUri( new URIish("git@github.com:jeschkies/dcos.git") )
  cmd.call()

  git.push()
    .setRemote("jeschkies")
    .setRefSpecs(new RefSpec(s"${commit.getId.getName}:refs/heads/marathon/latest"))
    .setForce(true) // We may want to rebase before and not force push.
    .call()
}

@main
def all(): Unit = {
  checkout()
  upgrade(
    "https://s3.amazonaws.com/downloads.mesosphere.io/marathon/snapshots/marathon-1.5.0-SNAPSHOT-637-gb17f6fe.tgz",
    "eb2665203afe5611e14c2bdbbf367917eece19db"
  )
  commitAndPush("1.5.0-SNAPSHOT-637-gb17f6fe")
}
