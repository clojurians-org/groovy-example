```
grails create-app nest_rest --profile=rest-api
grails create-domain-class author
grails create-domain-class book
grails generate-all nest_rest.Author
grails generate-all nest_rest.Book 

// UrlMappings.groovy:
        delete "/$pname/$pid/$controller/$id(.$format)?"(action:"delete")
        get "/$pname/$pid/$controller(.$format)?"(action:"index")
        get "/$pname/$pid/$controller/$id(.$format)?"(action:"show")
        post "/$pname/$pid/$controller(.$format)?"(action:"save")
        put "/$pname/$pid/$controller/$id(.$format)?"(action:"update")
        patch "/$pname/$pid/$controller/$id(.$format)?"(action:"patch")


// Author.groovy
class Author {
    List books
    static hasMany = [books: Book]

    String name

    static constraints = {
        name(unique:true, nullable:false)
    }
}

// Book.groovy
class Book {
    static belongsTo = [author: Author]
    String title

    static constraints = {
    }
}

// BookController.groovy
    def index(Integer max) {
        params.max = Math.min(max ?: 10, 100)
        def books = Author.get(params.pid)?.books ?: []
        respond books, model:[bookCount: books.size()]
    }
    def book = new Book(originBook.properties + [author:Author.get(params.pid)])


grails run-app -host=0.0.0.0 -port=1111

curl -X POST -L -H "Content-Type: application/json" -d '{"name":"larluo1"}' http://192.168.1.2:1111/author
curl -X POST -L -H "Content-Type: application/json" -d '{"name":"larluo2"}' http://192.168.1.2:1111/author

curl -X POST -L -H "Content-Type: application/json" -d '{"title":"larluo-book-1"}' http://192.168.1.2:1111/author/1/book
curl -X POST -L -H "Content-Type: application/json" -d '{"title":"larluo-book-2"}' http://192.168.1.2:1111/author/2/book

```

