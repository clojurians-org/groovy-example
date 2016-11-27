#! /usr/bin/env groovy -Dorg.apache.logging.log4j.level=info -Dgroovy.grape.report.downloads=true -Divy.message.logger.level=1
/**********************************
 * [intro]
 *   author=larluo@spiderdt.com
 *   func=merge algorithm for data warehouse
 *=================================
 * [param]
 *   job_id=
 *=================================
 * [caller]
 *   spark_etl_merge.groovy d_bolome_product_category '[[":barcode", 0]]'
 *=================================
 * [version]
 *   v1_0=2016-09-20@larluo{create}
 *   v1_1=2016-10-10@larluo{add hive table autogeneration}
 **********************************/

@Grab('org.slf4j:slf4j-api:1.7.7')
@Grab('org.apache.logging.log4j:log4j-slf4j-impl:2.7')
@Grab('org.apache.logging.log4j:log4j-core:2.7')
@Grab('org.apache.logging.log4j:log4j-api:2.7')
@Grab('org.apache.spark:spark-core_2.11:2.0.0')
@Grab('org.apache.spark:spark-yarn_2.11:2.0.0')
@Grab('com.spiderdt.framework:spiderdt-client:0.1')

@GrabExclude('org.slf4j:slf4j-log4j12')

import org.slf4j.LoggerFactory
import java.security.CodeSource
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.CompilationUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DFSClient
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.TaskContext
import scala.Tuple2
import org.apache.spark.api.java.JavaPairRDD
import scala.collection.JavaConversions
import java.util.jar.JarOutputStream
import java.util.jar.JarEntry

import static thrift.hive.TCLIServiceClient.*

/**********************************
 * PARAMETER AREA
 **********************************/
def (tabname, merge_cols_str) = args

def hadoop_master = "192.168.1.3"
def hive_server2 = "192.168.1.3"

def client_args  =  [job_id: this.class.name,
                     whoami: System.getProperty("user.name"),
                     uuid: UUID.randomUUID(),
                     tabname: tabname, 
                     hadoop_master: hadoop_master,
                     dfs_root: "hdfs://${hadoop_master}:9000".toString(),
                     dfs_client: new DFSClient(new URI("hdfs://${hadoop_master}:9000".toString()), new Configuration()),
                     hive_info: [hostname: hive_server2],
                     hive_dir: "/user/hive/warehouse"]
def spark_args = [dfs_client_info: [root: "hdfs://${hadoop_master}:9000".toString()],
                  merge_cols: Eval.me(merge_cols_str.replace(":", "")),
                  in_path: "/${client_args.hive_dir.drop(1)}/stg.db/${client_args.tabname}".toString(),
                  out_path: "/user/${client_args.whoami}/${client_args.uuid}/${client_args.hive_dir.drop(1)}/ods.db/${client_args.tabname}".toString()]
               

/**********************************
 * ENV SETUP AREA
 **********************************/
// init log
def log = LoggerFactory.getLogger(this.class)
System.setOut(new PrintStream(System.out) { void print(String str) {log.info(str)} })
System.setErr(new PrintStream(System.err) { void print(String str) {log.error(str)} })
3.times {log.info ""}

// classloader parse and upload jar
class MyGroovyClassLoader extends GroovyClassLoader {
    CompilationUnit compilationUnit ;
    CompilationUnit createCompilationUnit(CompilerConfiguration config, CodeSource source) {
        compilationUnit = super.createCompilationUnit(config, source)
    }
}
def hdfs_jar_path = "/user/spiderdt/spark_yarn_archive/onetime_${this.class.name}.jar".toString()
new MyGroovyClassLoader().with {loader ->
  loader.parseClass(new File(this.class.protectionDomain.codeSource.location.path))
  def bos = new ByteArrayOutputStream()
  new JarOutputStream(bos).with {jar ->
    loader.compilationUnit.classes.each{ log.trace(it.name); jar.putNextEntry(new JarEntry(it.name + ".class")); jar.write(it.bytes) }
    jar.close()
  }
  client_args.dfs_client.create(hdfs_jar_path, true).with{ bos.writeTo(it); it.close() }
}

/**********************************
 * SPARK CODE
 **********************************/
class MergeClient {
  static log = LoggerFactory.getLogger(MergeClient.class)
  static list_dirs(dfs_client, filepath) {
      dfs_client.listPaths(filepath).getPartialListing().grep{it.dir}*.getFullName(filepath)
  }
  static list_leaf_dirs (dfs_client, filepath) {
      list_dirs(dfs_client, filepath).with {it ? it.collect{subDir -> list_leaf_dirs(dfs_client, subDir)}.flatten() : [filepath]} ?: filepath
  }
  static create_hive_tab (hive_info, tabname, prt_cols, data_cols) {
    log.info "HIVE TABLE:" + [tabname: tabname, prt_cols: prt_cols, data_cols: data_cols]
    def prt_cols_part = prt_cols ? ("PARTITIONED BY ( " + prt_cols.collect{"`${it}` STRING"}.join(", ") + " )") : ""
    def data_cols_part = data_cols.collect{"`${it}` STRING"}.join(", ")
    def ddl_sql = "CREATE EXTERNAL TABLE IF NOT EXISTS ${tabname} ( ${data_cols_part} ) ${prt_cols_part}"
    log.info "RUN DDL: " + ddl_sql
      
    openHiveClient(hive_info.hostname, 10000, hive_info.username ?: 'spiderdt', hive_info.password ?: 'spiderdt').with {
      executeStatement(it, ddl_sql)
      closeHiveClient(it)
    }
  }

  static run(args, spark_args) {
    log.info "params:" +  [tabname: args.tabname, uuid: args.uuid, merge_cols: spark_args.merge_cols]
    // SPARK CONF
    def sc = new JavaSparkContext(
        new SparkConf().each {
            it.setAll(JavaConversions.mapAsScalaMap(
                ["spark.app.name": [args.job_id, args.tabname].join("-").toString(),
                 "spark.master": "yarn",
                 "spark.yarn.am.memory": "1g",
                 "spark.executor.instances": "1",
                 "spark.executor.memory": "2g",
                 "spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive": "true",
                 "spark.hadoop.yarn.resourcemanager.hostname": args.hadoop_master,
                 "spark.hadoop.fs.defaultFS": args.dfs_root,
                 "spark.yarn.archive": args.dfs_root + "/user/spiderdt/spark_yarn_archive"
                ]
       ))}
    )

    // CREATE SPARK HIVE TABLE
    def stg_header_str = sc.wholeTextFiles("${args.hive_dir}/stg.db/${args.tabname}").map{it._2.split("\n")[0]}.collect().toSet()
    if (stg_header_str.size() != 1) throw new Exception("data header didn't match!")
    create_hive_tab(args.hive_info, "ods.${args.tabname}", [], 
                   ["prt_path", "dw_id", *stg_header_str[0].split(",").toList().withIndex(1).collect{val, idx ->  val ?: "X_${idx}"}])

    // FIND MAX SURROGATE ID
    spark_args.maxOdsId = sc.wholeTextFiles(spark_args.out_path.replaceAll("^/user/${args.whoami}/${args.uuid}", ""))
                            .mapPartitions {
                              it.collect{ // PER_FILE
                                it._2.split("\n").collect{row -> /* surrogate_key */ row.split("\001")[1] }.max()
                              }.max().iterator()
                            }.collect().max() ?: 0 as int
    log.info("maxOdsId: " + spark_args.maxOdsId)

    // JOIN STG ODS
    def stgRdd = sc.wholeTextFiles(spark_args.in_path)
                   .mapPartitions {
                     it.collectMany{ // PER_FILE
                       it._2.split("\n").drop(1).collect{row ->
                         row.split(",").with {flds ->  
                           [spark_args.merge_cols.collect{/*m_key, m_val*/[it[0], flds[it[1]]]}, flds.join("\001")]
                         }.with {new Tuple2(it[0], it[1])}
                       }
                     }.iterator()
                   }
    log.info("stgRddTake: " + stgRdd.take(2))
    def odsRdd  = sc.wholeTextFiles(spark_args.out_path.replaceAll("^/user/${args.whoami}/${args.uuid}", ""))
                   .mapPartitions {
                     it.collectMany { // PER_FILE
                       it._2.split("\n").collect{row ->
                         row.split("\001").with { flds ->  
                           [spark_args.merge_cols.collect{/*m_key, m_val*/[it[0], flds[it[1]+2]]}, row]  /*empty_prt_path + surrogate_key, line */
                         }.with {new Tuple2(it[0], it[1])}
                       }
                     }.iterator()
                   }
    log.info("odsRddTake: " + odsRdd.take(2))
    def fullRdd = JavaPairRDD.fromJavaRDD(stgRdd).fullOuterJoin(JavaPairRDD.fromJavaRDD(odsRdd)).cache()
    log.info("fullRddcOUNT: " + fullRdd.count())
    def existRdd= fullRdd.mapPartitions { 
                    it.findAll{it._2._2.orNull()}
                      .collect{it._2._1.orNull() ?: it._2._2.orNull()}.collect{[/*empty_prt_key*/"", it]}.iterator() 
                  }
    log.info("existRddPrtCount: " + existRdd.count())
    log.info("existRddPrtTake: " + existRdd.take(2))
    def newRdd = fullRdd.mapPartitions { it.findAll{!it._2._2.orNull()}.collect{it._2._1.orNull()}.iterator() }.cache()
    log.info("newRddPrtTake: " + newRdd.take(2))

    // CALCULATE SURROGATE KEY FOR NEW RDD DATA
    spark_args.newRddPrtCount = newRdd.mapPartitions { [[(TaskContext.get().partitionId()): it.size()]].iterator() }.collect().collectEntries{it}
    log.info("newRddPrtCount: " + spark_args.newRddPrtCount.toString())

    // WRITE TO HDFS
    newRdd.mapPartitions{
      it.withIndex(1).collect {row, idx ->
        def surrogate_key = spark_args.maxOdsId + (spark_args.newRddPrtCount.findAll{it.key < TaskContext.get().partitionId()}*.value.sum() ?: 0) + idx
        [/*empty_prt_key*/"", ["", surrogate_key, row].join("\001")] /*empty_prt_path + surrogate_key, line */
      }.iterator()
    }.union(existRdd).foreachPartition {
        def out_streams  = [:]
        def dfs_client = new DFSClient(new URI(spark_args.dfs_client_info.root), new Configuration())
        it.each {row ->
          // generate output path according to the [partition fields + rdd partition no]
          def out_path="${spark_args.out_path}/${row[0]}/data.csv.${TaskContext.get().partitionId()}".replace("//", "/").toString()
          if(!out_streams[out_path]) out_streams[out_path] = dfs_client.create(out_path, true)
          out_streams[out_path] << row[1].getBytes("UTF-8") << "\n"
        }
        out_streams.values()*.close()
      }


    // PERSIST RESULT
    list_leaf_dirs(args.dfs_client, spark_args.out_path).each {
        def tgt_path=it.replaceAll("^/user/${args.whoami}/${args.uuid}", "")
        log.info "persist to directory: ${tgt_path}"
        args.dfs_client.mkdirs(tgt_path.replaceAll('(.*)/[^/]*', '$1'))
        args.dfs_client.delete(tgt_path)
        args.dfs_client.rename(it, tgt_path)
    }
    args.dfs_client.delete("/user/${args.whoami}/${args.uuid}")
  }
}
MergeClient.run(client_args, spark_args)
