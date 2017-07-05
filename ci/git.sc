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
def upgrade(): Unit = {
  val buildinfoPath = pwd / 'dcos / 'packages/ 'marathon / "buildinfo.json"
  val buildinfoData = read(buildinfoPath)

  val buildinfo: upickle.Js.Obj = upickle.json.read(buildinfoData).asInstanceOf[upickle.Js.Obj]

  val updated = update("single_source", { case o: upickle.Js.Obj => update( "url", { case e: upickle.Js.Str => e.copy(value ="new_url") } )(o); case i => i } )(buildinfo)

  write.over(buildinfoPath, upickle.json.write(updated))
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
def commitAndPush(): Unit = {
  val dcosRepoPath = pwd / 'dcos
  val buildinfoPath = dcosRepoPath / 'packages/ 'marathon / "buildinfo.json"

  val git = new Git(new FileRepository( (dcosRepoPath / ".git").toIO ))

  git.add().addFilepattern(buildinfoPath.toString).call()
  val commit = git.commit().setMessage("Bump Marathon version to 123").call()

  val cmd = git.remoteAdd()
  cmd.setName("jeschkies")
  cmd.setUri( new URIish("git@github.com:jeschkies/dcos.git") )
  cmd.call()

  git.push()
    .setRemote("jeschkies")
    .setRefSpecs(new RefSpec(s"${c.getId.getName}:refs/heads/marathon/head"))
    .setForce(true) // We may want to rebase before and not force push.
    .call()
}
