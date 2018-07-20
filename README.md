# DemoOnScala

Steps For Installation:


 1.install scala plugin.
 name : Scala IDE
Location: http://download.scala-ide.org/sdk/lithium/e44/scala211/stable/site
 2. install scala-maven plugin.
 "Maven for Scala" - http://alchim31.free.fr/m2e-scala/update-site
 
 
 spark-submit --class com.scala.krishna.hiveEtl --master yarn --driver-memory 8G --executor-memory 8G --deploy-mode cluster --num-executors 2 --executor-cores 2 /hivestage/frdv/dev/lib/epms_lib/bookStructure/ScalaTest-0.0.1.jar 20180228 SG