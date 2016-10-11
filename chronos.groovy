@Grab("org.codehaus.groovy.modules.http-builder:http-builder:0.7.2")
import groovyx.net.http.HTTPBuilder
import static groovyx.net.http.Method.GET
import static groovyx.net.http.Method.PUT
import static groovyx.net.http.Method.VALUE
import static groovyx.net.http.Method.POST
import static groovyx.net.http.Method.DELETE
import static groovyx.net.http.ContentType.JSON
import static groovyx.net.http.ContentType.VALUE
import static groovyx.net.http.ContentType.TEXT


def http = new HTTPBuilder('http://192.168.1.2:8080')
http.ignoreSSLIssues()


def create_scheduled_job(http,xmap){
  default_map = [command: "echo dummy", schedule: "R10//P"]
  http.request(POST,JSON){req ->
    uri.path = '/scheduler/iso8601'
    body = default_map + xmap
  }
}

def create_dependecy_job(http, xmap){
 
  default_map = [command: "echo dummy"]
  http.request(POST,JSON){req ->
    uri.path = '/scheduler/dependency'
    body = default_map + xmap
  }
}

def start_job(http,job_name){
   http.request(PUT,JSON){req ->
     uri.path = "/scheduler/job/${job_name}"
   }
}

def graph_node(http, prefix){
   http.request(GET,TEXT){req ->
     uri.path = '/scheduler/graph/csv'
   }.text.split("\n").findAll{it.startsWith("node,${prefix}-")}.collect{it.split(",")[1]}
}
def graph_link(http, prefix) {
     http.request(GET,TEXT){req ->
     uri.path = '/scheduler/graph/csv'
   }.text.split("\n").findAll{it.startsWith("link,${prefix}-")}
  //.collect{it.split(",")[1..2]}.groupBy{it[0]}.collectEntries{k,v ->[k,v.collect{it[1]}]}
}
def delete_job(http,job_name){
    http.request(DELETE,JSON){req ->
    uri.path = "/scheduler/job/${job_name}"
 } 
}

def create_project(http, project_name, deps, cmds) {
  def dependency_jobs = deps.values().flatten().unique()
  def schedule_jobs = deps.keySet() - dependency_jobs
  schedule_jobs.each {
    println(" [INFO] create schedule job name: " + it)
    create_scheduled_job(http, [command:cmds[it], name: project_name + "-" + it])
  }
  // (k, vs) => (k,v)* => group v => (v, ks)
  def schedule_job_deps= deps.collect{
                               k,vs -> vs.collect{v -> [k, v]}
                             }.inject([]){ 
                               merge, item -> merge + item
                             }.groupBy{ 
                               it[1] 
                             }.collectEntries{ 
                               k, vs -> [k, vs.collect{it[0]}]
                             }
  dependency_jobs.each{
    println(" [INFO] create dep job name: " + it)
    create_dependecy_job(http,[name: project_name + "-" + it, parents: schedule_job_deps[it].collect{ project_name + "-" + it}])
  }
   println( " [OK] create project success!" )  
}

def run_project(http, project_name) {
   def deps = graph_link(http,project_name)
   println("deps:" + deps)
   def dependency_jobs = deps.values().flatten().unique()
   def schedule_jobs = deps.keySet() - dependency_jobs
   schedule_jobs.each{
        println(" [INFO] run schedule job: " + it)
        start_job(http,it)
   }
   println( " [OK] run project success!" )  
}


def delete_project(http, project_name) {
  // delete the job start with [projectname-] prefix
    def jobs = graph_node(http,project_name) 
    println("jobs:" + jobs)
    jobs.each{

        println(" [INFO] run delete job:" + it)
      delete_job(http,it)
    }
    println("[OK] delete project success!") 
}  
// println(create_scheduled_job(http,[command: "echo 111111111", name: "test"]))
//println(create_dependecy_job(http,[command:"chong-bolome_done", name:"chong-bolome_done", 
//    parents:["bolome_dau", "bolome_events", "bolome_inventory", "bolome_orders", "bolome_product_category", "bolome_shows"]]))
// println(start_job(http,"test"))
// println(graph_csv(http, 'test'))
//println( delete_project(http, "chong") )
/*
create_project(http, "chong",
                           [bolome_ftp2hdfs: ['bolome_dau', 'bolome_events','bolome_inventory','bolome_orders','bolome_product_category','bolome_shows'],
                            bolome_dau : ['bolome_done'],
                            bolome_events : ['bolome_done'],
                            bolome_inventory : ['bolome_done'],
                            bolome_orders : ['bolome_done'],
                            bolome_product_category : ['bolome_done'],
                            bolome_shows : ['bolome_done']],
                           [bolome_ftp2hdfs: 'echo dummy',
                            bolome_dau: 'echo "a"',
                            bolome_envents :'echo "b"',
                            bolome_inventory : 'echo "c"',
                            bolome_orders : 'echo "d"',
                            bolome_product_category : 'echo "e"',
                            bolome_shows: 'echo "f"']
                           
)
*/

// println(graph_node(http, 'chong'))
// run_project(http, 'chong')
// delete_project(http,'chong')
// create_scheduled_job(http, [name: "jobrunner-test"])
// create_scheduled_job(http, [name: "larluo-test", command: "cat 1", schedule: "R//PT2H"])
create_scheduled_job(http, [name: "larluo-test2", command: """/home/spiderdt/work/git/spiderdt-release/groovy/expr_job.groovy state_store-ccc_exprJob '{"classId":"ccc","data_sources":{"model.d_bolome_inventory":{"column_items":[{"data_item":"snapshot_date","data_type":"date"},{"data_item":"warehouse_id","data_type":"string"},{"data_item":"barcode","data_type":"string"},{"data_item":"stock","data_type":"int"}],"driven_items":[{"data_type":"date","data_item":"snapshot_date"},{"data_type":"int","data_item":"stock"}],"target_items":[{"data_type":"string","data_item":"barcode"}]}}}'""", schedule: "R//PT2H"])
