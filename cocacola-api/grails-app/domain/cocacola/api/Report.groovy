package cocacola.api

class Report implements Serializable {
    String category
    String report
    String selector
    String data
    static mapping = {
        table 'cocacola_rpt'
        id composite: ['report', 'category', 'selector']
        version false
        autoTimestamp false
    }

    static constraints = {
    }
}
