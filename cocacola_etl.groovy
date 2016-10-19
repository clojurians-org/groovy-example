#! /bin/groovy -Dgroovy.grape.report.downloads=true -Divy.message.logger.level=1

@Grab('org.apache.poi:poi:3.15')
@Grab('org.apache.poi:poi-ooxml:3.15')
@Grab('mysql:mysql-connector-java:6.0.3')


import org.apache.poi.xssf.usermodel.XSSFWorkbook
import static org.apache.poi.ss.usermodel.Cell.*
import org.apache.poi.ss.usermodel.CellType

import java.nio.file.Paths
import groovy.time.TimeCategory

import static groovy.json.JsonOutput.toJson
import groovy.sql.Sql

getClass().classLoader.rootLoader.addURL(new URL("file:///home/spiderdt/.m2/repository/mysql/mysql-connector-java/6.0.3/mysql-connector-java-6.0.3.jar"))

def parse_xls(xls_path, sheetname) {
  Paths.get(xls_path).withInputStream { input ->
    def score_sheet = new XSSFWorkbook(input).getSheet(sheetname)
    def header = score_sheet.getRow(0).cellIterator()*.stringCellValue.takeWhile{it != "_"}
    [["bottler_cn", "bottler_en", *header], 
     score_sheet.rowIterator().drop(3).collect{ 
        it.cellIterator().collect{ [(it.cellTypeEnum in  [CellType.NUMERIC]) ? it.numericCellValue.toString() : it.stringCellValue, it.address.column] }
     }]
  }
}

def to_map(dataset) {
    def (header, matrix) = dataset
    matrix.collect {
      it.collectEntries{ val, idx -> [header[idx], val] }
    }
}
def kpi_ly(kpi_date)  { 
  def (kpi, date) = kpi_date.split("_", 2) 
  kpi + "_Dec" + use (TimeCategory) { (Date.parse( 'MMMyy', date) - 12.month).format('yy') }
}
def kpi_pp(kpi_date) { 
  def (kpi, date) = kpi_date.split("_", 2) 
  kpi + "_" + use (TimeCategory) { (Date.parse( 'MMMyy', date) - 1.month).format('MMMyy') }
}
def lookup_cols(datamap) {
    datamap.collect {row ->
      row.collectEntries {k, v ->
        k in ['bottler_cn', 'bottler_en'] ? [k,v] : [k, [v, v - (row[kpi_pp(k)] ?: 0), v - (row[kpi_ly(k)] ?: 0)]]
      }
    }
}
def explode_cols(datamap, bottler_group) { 
  def botter_group_mapping = bottler_group.collectMany{it.value.collect{a_value-> [a_value, it.key]}}.collectEntries{it}
  datamap.collectMany { 
    def (bottler, channel) = it['bottler_en'].split("_", 2)
    it.findAll{!(it.key in ['bottler_cn', 'bottler_en'])}.collect {k, v ->
      def (kpi, date) = k.split("_", 2)
      [Date.parse( 'MMMyy', date).format('YYYY-MM'), botter_group_mapping[bottler], bottler, channel, kpi, *v]
    }
  }
}

def cal_expr(dataset, exprs) {
    def ret = [:]
    dataset.each {row -> 
        // println("row:" + row)
        exprs.each {
            if(it['dimension'].grep{it[0][0] != ':'}.every{ row[it[1]] == it[0] }) {
                def rpt_category =  it['dimension'].grep{it[0][0] == ':'}
                def filter_val = it['filter'].collect{row[it[1]]}
                def dimension_val = it['dimension'].grep{it[0][0] == ':'}.collectEntries{[it[0].drop(1), row[it[1]]]}
                def metrics_val = it['metrics'].collectEntries{[it[0].drop(1), row[it[1]]]}

                def filter_map = [(filter_val): (ret?.get(rpt_category)?.get(filter_val) ?: []) + [[dimension: dimension_val, metrics_val: metrics_val]]]
                def category_map = [(rpt_category): (ret?.get(rpt_category) ?: [:]) + filter_map]
                ret += category_map
            }
        }
    }
    ret
}

def to_mysql(category_map, mysql_info) {
  Sql.loadDriver('com.mysql.cj.jdbc.Driver')
  println("[info] writing to mysql...")
  def sql = Sql.newInstance(mysql_info)
  sql.execute "CREATE TABLE IF NOT EXISTS cocacola_rpt ( name VARCHAR(64), category VARCHAR(1000), filter VARCHAR(1000), data VARCHAR(10000) );"

  sql.withBatch(100, "REPLACE INTO cocacola_rpt(name, category, filter, data) VALUES('score', ?, ?, ?)") {
    category_map.take(2).each {category, filter_map->
      filter_map.each {filter, data ->
        println("insert:" + [category: category, filter: filter, data: data])
        it.addBatch(toJson(category), toJson(filter), toJson(data)) 
      }
    }
  }
}

def cocacola_xls = "/home/spiderdt/work/git/spiderdt-working/larluo/score.xlsm"

// date, bottler_group, bottler, channel, kpi, score, score_pp, score_lp
def exprs = [ [filter : [[":date", 0], [":bottler_group", 1], [":bottler", 2]], 
               dimension : [["Total", 3], ["Total", 4]], 
               metrics : [[":score", 5], [":score_pp", 6], [":score_lp", 7]]],
              [filter : [[":date", 0], [":bottler_group", 1], [":bottler", 2]], 
               dimension : [[":channel", 3], ["Total", 4]], 
               metrics : [[":score", 5]]], 
              [filter : [[":date", 0], [":bottler_group", 1], [":bottler", 2]], 
               dimension : [["Total", 3], [":kpi", 4]], 
               metrics : [[":score", 5]]], 
              [filter : [[":date", 0], [":bottler_group", 1], [":bottler", 2]], 
               dimension : [[":channel", 3], ["Total", 4], [":bottler", 2]], 
               metrics : [[":score", 5]]], 
              [filter : [[":date", 0], [":bottler_group", 1], [":bottler", 2]], 
               dimension : [["Total", 3], [":kpi", 4], [":bottler", 2]], 
               metrics : [[":score", 5]]] 
            ]
def mysql_info = [url: "jdbc:mysql://192.168.1.2:3306/state_store?useSSL=false",
                  user: "state_store",
                  password: "spiderdt",
                  driver: "com.mysql.cj.jdbc.Driver"
                  ]
def bottler_group = [China : ['China'],
                     BIG: ['BIG','LNS','GX','YN','Shanxi','LNN','SH','HB','SC','CQ','HLJ','JL'],
                     CBL: ['CBL','HaiN','BJ','ZM','SD','HeB','JX','TJ','HuN','InnM','GS','XJ'],
                     SBL: ['SBL','AH','ZJ','FJ','Shaanxi','HeN','JS','GDW','GDE'],
                     Zhuhai: ['ZH']
                    ]
cocacola_xls.with{ parse_xls(it, "score") }
            .with{ to_map(it) }
            .with{ lookup_cols(it) }
            .with{ explode_cols(it, bottler_group) }
            .with{ cal_expr(it, exprs) }
            .with{ to_mysql(it, mysql_info) }
