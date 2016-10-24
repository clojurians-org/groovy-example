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
    }

    def getJobs(String projectId, String categoryId) {
    }
}
