package scheduler.api

import grails.transaction.Transactional

import groovyx.net.http.HTTPBuilder
import static groovyx.net.http.Method.GET
import static groovyx.net.http.Method.PUT
import static groovyx.net.http.Method.VALUE
import static groovyx.net.http.Method.POST
import static groovyx.net.http.Method.DELETE
import static groovyx.net.http.ContentType.JSON
import static groovyx.net.http.ContentType.VALUE
import static groovyx.net.http.ContentType.TEXT

@Transactional
class ChronosService {
    
    def http
    ChronosService() {
        http = new HTTPBuilder('http://192.168.1.2:8080')
        http.ignoreSSLIssues()
    }

    def graph_node(){
        http.request(GET,TEXT){req ->
            uri.path = '/scheduler/graph/csv'
        }.text.split("\n").findAll{it =~ /^node,\w+-\w+-/}.collect{it.split(",")[1]}
    }

    def getProjects() {
        graph_node().collect{it.split("-")[0]}.unique()
    }

    def getCategories(projectId) {
        graph_node().findAll{it.startsWith(projectId + "-")}.collect{it.split("-")[1]}.unique()
    }

    def getJobs(projectId, categoryId) {
         graph_node().findAll{it.startsWith(projectId + "-" + categoryId + "-")}.collect{it.split("-", 3)[2]}.unique()
    }

    def createJob(projectId, categoryId, name, params) {
        http.request(POST,JSON){req ->
            uri.path = '/scheduler/iso8601'
            body = [name: "${projectId}-${categoryId}-${name}".toString()] + params
        }
    }
    def createNowOnetimeJob(projectId, categoryId, name, command) {
        createJob(projectId, categoryId, name, [command: command, schedule: "R/9999-99-99T00:00:00Z/P"])
        http.request(PUT,JSON){req ->
            uri.path = "/scheduler/job/${projectId}-${categoryId}-${name}".toString()
        }
    }
    def createNowPeriodJob(projectId, categoryId, name, hms, command) {
        createJob(projectId, categoryId, name, [command: command, schedule: "R//PT${hms}".toString()])
    }
    def createOnetimeJob(projectId, categoryId, name, command, schedule) {
    }
    def createPeriodJob(projectId, categoryId, name, command, schedule) {
    }
}
