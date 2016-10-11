package nest_rest

class UrlMappings {

    static mappings = {
        delete "/$controller/$id(.$format)?"(action:"delete")
        get "/$controller(.$format)?"(action:"index")
        get "/$controller/$id(.$format)?"(action:"show")
        post "/$controller(.$format)?"(action:"save")
        put "/$controller/$id(.$format)?"(action:"update")
        patch "/$controller/$id(.$format)?"(action:"patch")

        delete "/$pname/$pid/$controller/$id(.$format)?"(action:"delete")
        get "/$pname/$pid/$controller(.$format)?"(action:"index")
        get "/$pname/$pid/$controller/$id(.$format)?"(action:"show")
        post "/$pname/$pid/$controller(.$format)?"(action:"save")
        put "/$pname/$pid/$controller/$id(.$format)?"(action:"update")
        patch "/$pname/$pid/$controller/$id(.$format)?"(action:"patch")

        "/"(controller: 'application', action:'index')
        "500"(view: '/error')
        "404"(view: '/notFound')
    }
}
