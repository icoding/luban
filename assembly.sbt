import AssemblyKeys._ // put this at the top of the file

assemblySettings

// your assembly settings here

mergeStrategy in assembly := {
  //这步是用来discard掉META-INF中的信息，这样就不会把原jar包中的签名信息打包到新包里，避免了出现java.lang.SecurityException: Invalid signature file digest for Manifest main attributes的报错
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  //这步是给所有其他冲突设置了合并策略（只留下第一个）
  case _ => MergeStrategy.first
}