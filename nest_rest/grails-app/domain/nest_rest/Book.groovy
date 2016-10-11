package nest_rest

class Book {
    static belongsTo = [author: Author]
    String title

    static constraints = {
    }

}
