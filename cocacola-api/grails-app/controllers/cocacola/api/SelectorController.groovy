package cocacola.api

import static org.springframework.http.HttpStatus.*
import grails.transaction.Transactional
import static groovy.json.JsonOutput.toJson



@Transactional(readOnly = true)
class SelectorController {

    static responseFormats = ['json', 'xml']
    static allowedMethods = [save: "POST", update: "PUT", delete: "DELETE"]

    static tree(data) {
      data.groupBy{it.size() == 1}.collectEntries {
        it.key ? [items : it.value.collectMany{it}] : it.value.groupBy{it[0]}.collectEntries { [it.key, tree(it.value*.drop(1))] }
      }
    }

    def index() {
        render toJson(tree(Report.executeQuery(
                   "SELECT selector FROM Report WHERE ${params.ppname} = '${params.ppid}' AND ${params.pname} = '${params.pid}'"
        ).collect{Eval.me(it)}))
    }

    def show(String selector) {
        render toJson([info: "Not Implemented!"])
    }
}
