// Databricks notebook source

import scala.util.control._

// COMMAND ----------

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "3c799d27-9ca5-4d08-b10d-751c13902784",
  "fs.azure.account.oauth2.client.secret" -> ".EyH5-9qg2lR2B3I~P_M0-3F0pmN_U_rl3",
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/eed40802-39ad-482f-9255-8f05f0209180/oauth2/token")


val mounts = dbutils.fs.mounts()
val mountPath = "/mnt/data"
var isExist: Boolean = false
val outer = new Breaks;

outer.breakable {
  for(mount <- mounts) {
    if(mount.mountPoint == mountPath) {
      isExist = true;
      outer.break;
    }
  }
}



// COMMAND ----------

if(isExist) {
  println("Volume Mounting for Case Study Data Already Exist!")
}
else {
  dbutils.fs.mount(
    source = "abfss://casestudydata@karthikadls.dfs.core.windows.net/",
    mountPoint = "/mnt/data",
    extraConfigs = configs)
}



