#! /bin/groovy
/**********************************
 * [intro]
 *   author=larluo@spiderdt.com
 *   func=partition algorithm for data warehouse
 *=================================
 * [param]
 *   tabname=staging table name
 *   prt_cols_str=ods partition cols
 * ex: def (tabname, prt_cols_str) = ['d_bolome_inventory',  '[[":p_date", 0]]']
 *=================================
 * [caller]
 *   d_bolome_dau [[":p_date", 1]]
 *   d_bolome_events
 *   d_bolome_inventory
 *   d_bolome_orders
 *=================================
 * [version]
 *   v1_0=2016-09-28@larluo{create}
 **********************************/

@Grab('org.apache.spark:spark-core_2.11:2.0.0')
@Grab('org.apache.spark:spark-yarn_2.11:2.0.0')
@Grab('log4j:log4j:1.2.17')
import java.security.CodeSource
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.CompilationUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DFSClient
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.TaskContext
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


def (job_id) = ["spark-etl.prt"]

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
    log.root.addAppender(
      new DailyRollingFileAppender(
        new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n"), 
        "/home/spiderdt/work/git/spiderdt-release/var/log/${job_id}.log",
        "'.'yyyy-MM-dd"
      )
    )
}
3.times {log.info ""}

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
 * PACKAGE JAR AND SUBMIT
 **********************************/
def jar_path = "/home/spiderdt/work/git/spiderdt-release/var/jar/${getClass().getName()}.jar"
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

def (tabname, prt_cols_str) = args
SparkSubmit.main(
    ["--master", "yarn", 
     "--deploy-mode", "client", 
     "--class", "Prt",
     "--jars", "/home/spiderdt/work/git/spiderdt-release/target/groovy-all-2.4.7.jar",
     jar_path,
     tabname, prt_cols_str
    ] as String[])

/**********************************
 * SPARK CODE
 **********************************/
class Prt {
  static log = Logger.getInstance(Prt.class)
  static list_dirs(dfs_client, filepath) {
      dfs_client.listPaths(filepath).getPartialListing().grep{it.dir}*.getFullName(filepath)
  }
  static list_leaf_dirs (dfs_client, filepath) {
      list_dirs(dfs_client, filepath).with {it ? it.collect{subDir -> list_leaf_dirs(dfs_client, subDir)}.flatten() : [filepath]} ?: filepath
  }
  static main(args) {
    // 1. PARSE PARAMETER
    def (tabname, prt_cols_str) = args
    def prt_cols = Eval.me(prt_cols_str.replace(":", ""))
    def (default_fs, username, uuid, hive_dir) = ["hdfs://192.168.1.3:9000", System.getProperty("user.name"), UUID.randomUUID(), "user/hive/warehouse"]
    log.info "params:" +  [tabname: tabname, prt_cols: prt_cols, uuid: uuid]

    // 2. RUN SPARK
    def sc = new JavaSparkContext (
        new SparkConf().with {
            it.setAll(JavaConversions.mapAsScalaMap(
                ["spark.app.name":"spark-etl.prt",
                 "spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive": "true",
                 "spark.hadoop.yarn.resourcemanager.hostname": "192.168.1.3",
                 "spark.hadoop.fs.defaultFS": default_fs]
            ))
        it}
    )
    sc.wholeTextFiles("/${hive_dir}/stg.db/${tabname}")
      .flatMap {
        it._2.split("\n").drop(1).collect{ 
          it.split(",").with {flds ->  prt_cols.collect{[it[0], flds[it[1]]]}*.join("=").join("/").with {[it, [it, *flds].join("\001")]} } 
        }.iterator()
      }.foreachPartition {
        def out_streams  = [:]
        def dfs_client = new DFSClient(new URI(default_fs), new Configuration())
        it.each {
          // generate output path according to the [partition fields + rdd partition no]
          def out_path="/user/${username}/${uuid}/${hive_dir}/ods.db/${tabname}/${it[0]}/data.csv.${TaskContext.get().partitionId()}"
          if(!out_streams[out_path]) out_streams[out_path] = dfs_client.create(out_path, true)
          out_streams[out_path] << it[1].getBytes("UTF-8") << "\n"
        }
        out_streams.values()*.close()
      }

    // 3. PERSIST DATA
    def dfs_client = new DFSClient(new URI(default_fs), new Configuration())
    list_leaf_dirs(dfs_client, "/user/${username}/${uuid}/${hive_dir}/ods.db/${tabname}").each {
        def tgt_path=it.replaceAll("^/user/${username}/${uuid}", "")
        log.info "persist to directory: ${tgt_path}"
        dfs_client.delete(tgt_path)
        dfs_client.rename(it, tgt_path)
    }
    dfs_client.delete("/user/${username}/${uuid}")
  }
}
