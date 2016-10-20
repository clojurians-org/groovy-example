package cocacola.api

import static org.springframework.http.HttpStatus.*
import grails.transaction.Transactional
import static groovy.json.JsonOutput.toJson

@Transactional(readOnly = true)
class CategoryController {
    static responseFormats = ['json', 'xml']
    static allowedMethods = [save: "POST", update: "PUT", delete: "DELETE"]

    def index() {
        render toJson(Report.executeQuery("SELECT DISTINCT category FROM Report"))
    }

    def show(String name) {
        render toJson([info: "Not Implemented!"])
    }
}
