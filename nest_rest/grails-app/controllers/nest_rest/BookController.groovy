package nest_rest

import static org.springframework.http.HttpStatus.*
import grails.transaction.Transactional

@Transactional(readOnly = true)
class BookController {

    static responseFormats = ['json', 'xml']
    static allowedMethods = [save: "POST", update: "PUT", delete: "DELETE"]

    def index(Integer max) {
        params.max = Math.min(max ?: 10, 100)
        def books = Author.get(params.pid)?.books ?: []
        respond books, model:[bookCount: books.size()]
    }

    def show(Book book) {
        respond book
    }

    @Transactional
    def save(Book originBook) {
        def book = new Book(originBook.properties + [author:Author.get(params.pid)])
        if (book == null) {
            transactionStatus.setRollbackOnly()
            render status: NOT_FOUND
            return
        }

        if (book.hasErrors()) {
            transactionStatus.setRollbackOnly()
            respond book.errors, view:'create'
            return
        }

        book.save flush:true

        respond book, [status: CREATED, view:"show"]
    }

    @Transactional
    def update(Book originBook) {
        def book = new Book(originBook.properties + [author:Author.get(params.pid)])
        if (book == null) {
            transactionStatus.setRollbackOnly()
            render status: NOT_FOUND
            return
        }

        if (book.hasErrors()) {
            transactionStatus.setRollbackOnly()
            respond book.errors, view:'edit'
            return
        }

        book.save flush:true

        respond book, [status: OK, view:"show"]
    }

    @Transactional
    def delete(Book originBook) {
        def book = new Book(originBook.properties + [author:Author.get(params.pid)])
        if (book == null) {
            transactionStatus.setRollbackOnly()
            render status: NOT_FOUND
            return
        }

        book.delete flush:true

        render status: NO_CONTENT
    }
}
