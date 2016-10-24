package scheduler.api

import grails.transaction.Transactional
import grails.rest.*
import grails.converters.*
import static groovy.json.JsonOutput.toJson

@Transactional(readOnly = true)
class JobController {
    def chronosService

    static responseFormats = ['json', 'xml']
    static allowedMethods = [save: "POST", update: "PUT", delete: "DELETE"]
	
    def index() {
        render toJson(chronosService.getJobs(params.ppid, params.pid))
    }
}





