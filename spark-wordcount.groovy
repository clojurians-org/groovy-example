@Grab('org.apache.spark:spark-core_2.11:2.0.0')
import org.apache.spark.api.java.*
import org.apache.spark.SparkConf
import org.apache.spark.api.java.function.Function
import scala.collection.JavaConversions

class SimpleApp {
  static main(args) {
     
    def logFile = "/Users/larluo/work/git/groovy-example/groovy-deps/README.md"
    def conf = new SparkConf ()
    conf.setAll(JavaConversions.mapAsScalaMap(["spark.master":conf.get("spark.master", "local[*]")] + ["spark.app.name":"Simple Application"]))
    def sc = new JavaSparkContext (conf)
    def logData = sc.textFile (logFile).cache ()

    println (
      logData.filter{ it.contains("a") }.count ()
    )

    def data = [1, 2, 3, 4, 5]
    println(sc.parallelize([1, 2, 3, 4, 5]).reduce{a,b -> a + b}.collect())
  }
}
