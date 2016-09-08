@Grab('org.apache.spark:spark-core_2.11:2.0.0')
@Grab('org.apache.spark:spark-yarn_2.11:2.0.0')
@Grab('log4j:log4j:1.2.17')
import org.apache.spark.api.java.*
import org.apache.spark.SparkConf
import org.apache.spark.api.java.function.Function
import scala.collection.JavaConversions
import groovy.util.logging.Log4j
import org.apache.log4j.*
import org.apache.spark.deploy.SparkSubmit

Logger log = Logger.getInstance(getClass())
log.root.level = Level.DEBUG
log.root.addAppender(new ConsoleAppender(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")))

@Log4j
class SimpleApp {
  static main(args) {

    log.info "main method is running..."
    def sc = new JavaSparkContext (new SparkConf())
    sc.hadoopConfiguration().with {
        it.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
        it.set("yarn.resourcemanager.hostname", "192.168.1.3")
    }
    println(
      sc.textFile("hdfs://192.168.1.3:9000/user/hive/warehouse/stg.db/d_bolome_product_category")
      .first()
    )
  }
}

SparkSubmit.main(["--name", "Simple Application", 
                  // "--master", "local[*]", 
                  "--master", "yarn", "--deploy-mode", "cluster", 
                  "--class", "SimpleApp",
                  "spark-internal"] as String[])
