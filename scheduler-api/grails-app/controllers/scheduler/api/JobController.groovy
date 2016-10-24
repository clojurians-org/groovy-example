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
    def save(Job job) {
        def job_dir = "/home/spiderdt/work/git/spiderdt-release/data-platform/groovy"
        def (projectId, categoryId) = [params.ppid, params.pid]
        def job_args = job.args ?: []
        switch(categoryId) {
            case 'expr': 
                chronosService.createNowPeriodJob(
                    projectId, categoryId, job.name, "12H", "${job_dir}/expr_job.groovy ${job_args.join(" ")}".toString()
                ) ; break
            case 'model':
                chronosService.createNowOnetimeJob(
                    projectId, categoryId, job.name, "${job_dir}/model_job.groovy ${job_args.join(" ")}".toString()
                ) ; break
            case 'dashboard':
                chronosService.createNowOnetimeJob(
                    projectId, categoryId, job.name, "${job_dir}/dashboard_job.groovy ${job_args.join(" ")}".toString()
                ) ; break
        }
        render toJson(job.properties + [projectId: projectId, categoryId:categoryId, id: job.name])
    }
}
