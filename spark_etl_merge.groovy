#! /bin/groovy
/**********************************
 * [intro]
 *   author=larluo@spiderdt.com
 *   func=merge algorithm for data warehouse
 *=================================
 * [param]
 *   job_id=
 *=================================
 * [version]
 *   v1_0=2016-09-20@larluo{create}
 **********************************/

@Grab('org.apache.spark:spark-core_2.11:2.0.0')
@Grab('org.apache.spark:spark-yarn_2.11:2.0.0')
@Grab('log4j:log4j:1.2.17')
import java.security.CodeSource
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.CompilationUnit

import org.apache.spark.api.java.*
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkSubmit
import scala.Tuple2
import scala.collection.JavaConversions
import java.util.jar.JarOutputStream
import java.util.jar.JarEntry
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.PatternLayout
import org.apache.log4j.DailyRollingFileAppender

def (job_id) = ["spark-etl.merge"]

/**********************************
 * LOG INITIALIZATION
 **********************************/
def log = Logger.getInstance(getClass())
System.setOut(new PrintStream(System.out) { void print(String str) {log.info(str)} })
System.setErr(new PrintStream(System.err) { void print(String str) {log.info(str)} })
log.root.with {
    it.level = Level.INFO
    it.removeAllAppenders()
    log.root.addAppender(new ConsoleAppender(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")))
/*
    log.root.addAppender(
      new DailyRollingFileAppender(
        new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n"), 
        "/home/spiderdt/work/git/spiderdt-release/var/log/${job_id}.log",
        "'.'yyyy-MM-dd"
      )
    )
*/
}
log.info "\n\n\n"

/**********************************
 * CLASS LOADER SETUP
 **********************************/
class MyGroovyClassLoader extends GroovyClassLoader {
    CompilationUnit compilationUnit ;
    CompilationUnit createCompilationUnit(CompilerConfiguration config, CodeSource source) {
        compilationUnit = super.createCompilationUnit(config, source)
    }
}

/**********************************
 * PACKAGE JAR ITSELF 
 **********************************/
def jar_path = "/home/spiderdt/work/git/spiderdt-release/var/jar/spark_etl_merge.jar"
def filename = getClass().getName()
new MyGroovyClassLoader().with {
    def bos = new ByteArrayOutputStream()
    def jar = new JarOutputStream(bos)
    it.parseClass(new File(getClass().protectionDomain.codeSource.location.path))
    it.compilationUnit.getClasses().each {
        jar.putNextEntry(new JarEntry(it.name + ".class"))
        jar.write(it.bytes)
    }
    jar.close()
    new File(jar_path).bytes = bos.toByteArray()
    bos.close()
}


/**********************************
 * SPARK CODE
 **********************************/
class Merge {
  static main(args) {
    def sc = new JavaSparkContext (
        new SparkConf().with {
            it.setAll(JavaConversions.mapAsScalaMap(
                ["spark.app.name":"spark-etl.merge",
                 "spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive": "true",
                 "spark.hadoop.yarn.resourcemanager.hostname": "192.168.1.3",
                 "spark.hadoop.fs.defaultFS": "hdfs://192.168.1.3:9000"]
            ))
        it}
    )
    def stgRdd = sc.wholeTextFiles("hdfs://192.168.1.3:9000/user/hive/warehouse/stg.db/d_bolome_product_category")
                   .flatMapToPair{
                       it._2.split("\n").drop(1)
                            .collect{
                              def fields = it.split(",")
                              new Tuple2(fields.first(), fields.join("\001"))
                            }.iterator()
                   }
    def odsRdd  = sc.wholeTextFiles("hdfs://192.168.1.3:9000/user/hive/warehouse/ods.db/d_bolome_product_category")
                   .flatMapToPair{
                       it._2.split("\n").drop(1)
                            .collect{
                               new Tuple2(it.split("\001").first(), it)
                            }.iterator()
                   }

      stgRdd.fullOuterJoin(odsRdd).map{
        it._2._1.orNull() ?: it._2._2.orNull()
      }.saveAsTextFile("hdfs://192.168.1.3:9000/user/.hive/warehouse/ods.db/d_bolome_product_category")

  }
}

SparkSubmit.main(
    ["--master", "yarn", 
     "--deploy-mode", "client", 
     "--class", "Merge",
     jar_path
    ] as String[])
