#! /bin/groovy -Dgroovy.grape.report.downloads=true -Divy.message.logger.level=1
/**********************************
 * [intro]
 *   author=larluo@spiderdt.com
 *   func=expression algorithm for data warehouse
 *=================================
 * [param]
 *=================================
 * [caller]
 *   chronos
 *=================================
 * [version]
 *   v1_0=2016-10-24@larluo{create}
 **********************************/

@GrabExclude('org.apache.hbase:hbase-client')
@GrabExclude('org.apache.hbase:hbase-annotations')
@GrabExclude('org.apache.hbase:hbase-hadoop2-compat')
@GrabExclude('org.apache.hbase:hbase-server')
@GrabExclude('org.apache.hbase:hbase-protocol')
@GrabExclude('org.apache.hbase:hbase-common')
@GrabExclude('org.apache.hbase:hbase-hadoop-compat')

@Grab('org.apache.thrift:libthrift:0.9.2')
@Grab('org.apache.hive:hive-service:2.1.0')
@Grab('org.apache.logging.log4j:log4j-core:2.6.2')
@Grab('org.apache.logging.log4j:log4j-api:2.6.2')

@Grab('org.apache.spark:spark-core_2.11:2.0.0')
@Grab('org.apache.spark:spark-yarn_2.11:2.0.0')

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

import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.TSaslClientTransport
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.hive.service.rpc.thrift.TCLIService
import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.hive.service.rpc.thrift.TOpenSessionReq
import org.apache.hive.service.rpc.thrift.TFetchResultsReq
import org.apache.hive.service.rpc.thrift.TGetOperationStatusReq
import org.apache.hive.service.rpc.thrift.TGetSchemasReq
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq
import org.apache.hive.service.rpc.thrift.TFetchOrientation
import org.apache.hive.service.auth.PlainSaslHelper

import static groovy.json.JsonOutput.toJson
/**********************************
 * PARAMETER AREA
 **********************************/
def (job_id) = ["spark-etl.expr"]
// def (tabs_str, tab_cols_str, exprs_str) = args
def tabs_str = """["model.d_bolome_orders", "4ml.larluo.d_bolome_orders"]"""
def tab_cols_str =  """["'prt_path", 
                        "'_pay_date", "'_user_id", "'_order_id", "'barcode", 
                        "quantity", "price", "'_warehouse_id", "'_show_id", 
                        "'_preview_show_id", "'_replay_show_id", "'_coupon_id", "'_event_id", 
                        "_copon_discount_amount", "_system_discount_amount", "_tax_amount", "_logistics_amount"]"""
def exprs_str = """[[":is_copon_discount", ":copon_discount_amount == 0"],
                    [":is_system_discount", ":system_discount_amount == 0.0"],
                    [":is_discount", ":is_copon_discount || :is_system_discount"]]"""

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

    // parse current script class
    it.parseClass(new File(getClass().protectionDomain.codeSource.location.path))
    it.compilationUnit.getClasses().each {
        jar.putNextEntry(new JarEntry(it.name + ".class"))
        jar.write(it.bytes)
    }
    // parse dynamic expr script class
    it.parseClass( new GroovyCodeSource(Eval.me(exprs_str)*.join("=").join("\n").replace(":", ""), "expr_script", GroovyShell.DEFAULT_CODE_BASE) )
    it.compilationUnit.getClasses().each {
        jar.putNextEntry(new JarEntry("expr_script.class"))
        jar.write(it.bytes)
    }
    jar.close()
    new File(jar_path).bytes = bos.toByteArray()
    bos.close()
}

SparkSubmit.main(
    ["--master", "yarn", 
     "--deploy-mode", "client", 
     "--class", "Client",
     "--executor-memory", "4g",
     jar_path,
     tabs_str, tab_cols_str, exprs_str
    ] as String[])

/**********************************
 * SPARK CODE
 **********************************/
class Client {
  static log = Logger.getInstance(Client.class)
  static list_dirs(dfs_client, filepath) {
      dfs_client.listPaths(filepath).getPartialListing().grep{it.dir}*.getFullName(filepath)
  }
  static list_leaf_dirs (dfs_client, filepath) {
      list_dirs(dfs_client, filepath).with {it ? it.collect{subDir -> list_leaf_dirs(dfs_client, subDir)}.flatten() : [filepath]} ?: filepath
  }
  static execute_hive_statement (hive_info, sql) {
      def transport = new TSaslClientTransport (
            'PLAIN',
            null,
            null,
            null,
            [:],
            new PlainSaslHelper.PlainCallbackHandler(hive_info.username ?: 'spiderdt', hive_info.password ?: 'spiderdt'),
            new TSocket(hive_info.hostname, 10000)
      )
      transport.open()
      def client = new TCLIService.Client(new TBinaryProtocol(transport))
      def sess_handle = client.OpenSession(new TOpenSessionReq()).sessionHandle
      def op_handle = client.ExecuteStatement(new TExecuteStatementReq(sess_handle, sql)).operationHandle
      def op_status = client.GetOperationStatus(new TGetOperationStatusReq(op_handle)).status.statusCode
      transport.close()
      op_status
  }
  static create_hive_tab (hive_info, tabname, prt_cols, data_cols) {
      log.info "HIVE TABLE:" + [tabname: tabname, prt_cols: prt_cols, data_cols: data_cols]
      def prt_cols_part = prt_cols ? ("PARTITIONED BY ( " + prt_cols.collect{"`${it}` STRING"}.join(", ") + " )") : ""
      def data_cols_part = data_cols.collect{"`${it}` STRING"}.join(", ")
      def ddl_sql = "CREATE EXTERNAL TABLE IF NOT EXISTS ${tabname} ( ${data_cols_part} ) ${prt_cols_part}"
      log.info "RUN DDL: " + ddl_sql
      execute_hive_statement(hive_info, ddl_sql)
  }

  static main(args) {
    // 1. PARSE PARAMETER
    def (tabs_str, tab_cols_str, exprs_str) = args
    def (src_tab_subdir, tgt_tab_subdir) = Eval.me(tabs_str).collect{ it.split("\\.").with { [it[0] + ".db", *it.drop(1)] }.join("/") }
    def (tab_cols, exprs) = [Eval.me(tab_cols_str), Eval.me(exprs_str)]
    def (default_fs, username, uuid, hive_info, hive_dir) = ["hdfs://192.168.1.3:9000", System.getProperty("user.name"), UUID.randomUUID(), [hostname: "192.168.1.2"], "user/hive/warehouse"]
    log.info "params:" +  [src_tab_subdir: src_tab_subdir, tgt_tab_subdir: tgt_tab_subdir, tab_cols: tab_cols, exprs: exprs, uuid: uuid]

    // 2. RUN SPARK
    def sc = new JavaSparkContext (
        new SparkConf().with {
            it.setAll(JavaConversions.mapAsScalaMap(
                ["spark.app.name":"spark-etl.expr",
                 "spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive": "true",
                 "spark.hadoop.yarn.resourcemanager.hostname": "192.168.1.3",
                 "spark.hadoop.fs.defaultFS": default_fs
                 "spark.yarn.archive": default_fs + "/user/spiderdt/spark_yarn_archive"
                ]
            ))
        it}
    )

    // 2.1 CAL EXPR
    sc.wholeTextFiles("/${hive_dir}/${src_tab_subdir}")
      .mapPartitions {
        def expr_script = new GroovyShell().parse( exprs*.join("=").join("\n").replace(":", ""), "expr_script" )
        it.collectMany {
          it._2.split("\n").collect{
            // remove _ symbol(stand for drop_column), convert type and binding
            def flds = it.split("\001")
            def data_map = [tab_cols*.replaceAll(/^(')?_?/, '$1'), flds].transpose().collectEntries {
                             it[0][0] == "'" ?  [it[0].drop(1), it[1]] : [it[0], it[1].isInteger() ? it[1].toInteger() : it[1].toDouble()]
                           }
            expr_script.with { it.binding = new Binding(data_map); it.run() }
            def drop_cols = tab_cols*.replaceAll(/^'/, '').findAll{ it.startsWith("_") }*.drop(1)
            [flds[0], toJson(expr_script.binding.variables.findAll{! drop_cols.contains(it.key)})]
          }
        }.iterator()
      }.foreachPartition {
        def out_streams  = [:]
        def dfs_client = new DFSClient(new URI(default_fs), new Configuration())
        it.each {
          // generate output path according to the [partition fields + rdd partition no]
          def out_path="/user/${username}/${uuid}/${hive_dir}/${tgt_tab_subdir}/${it[0]}/data.json.${TaskContext.get().partitionId()}"
          if(!out_streams[out_path]) out_streams[out_path] = dfs_client.create(out_path, true)
          out_streams[out_path] << it[1].getBytes("UTF-8") << "\n"
        }
        out_streams.values()*.close()
      }

    // 3. PERSIST DATA
    def dfs_client = new DFSClient(new URI(default_fs), new Configuration())
    list_leaf_dirs(dfs_client, "/user/${username}/${uuid}/${hive_dir}/${tgt_tab_subdir}").each {
        def tgt_path=it.replaceAll("^/user/${username}/${uuid}", "")
        log.info "persist to directory: ${tgt_path}"
        dfs_client.mkdirs(tgt_path.replaceAll('(.*)/[^/]*', '$1'))
        dfs_client.delete(tgt_path)
        dfs_client.rename(it, tgt_path)
    }
    dfs_client.delete("/user/${username}/${uuid}")
  }
}
