package nest_rest

class Author {
    List books
    static hasMany = [books: Book]

    String name

    static constraints = {
        name(unique:true, nullable:false)
    }
}
