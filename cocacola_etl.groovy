#! /bin/groovy -Dgroovy.grape.report.downloads=true -Divy.message.logger.level=1

@Grapes([
    @Grab('org.apache.poi:poi:3.15'),
    @Grab('org.apache.poi:poi-ooxml:3.15')])

import org.apache.poi.xssf.usermodel.XSSFWorkbook
import static org.apache.poi.ss.usermodel.Cell.*
import org.apache.poi.ss.usermodel.CellType

import java.nio.file.Paths

def parse_xls(xls_path, sheetname) {
  Paths.get(xls_path).withInputStream { input ->
    def score_sheet = new XSSFWorkbook(input).getSheet(sheetname)
    def header = score_sheet.getRow(0).cellIterator().collect{it.stringCellValue}.takeWhile{it != "_"}
    score_sheet.rowIterator().drop(3).collect {row ->
      row.cellIterator().collect{ [it, it.address.column] }.dropWhile{it[1] < 2}.collect {cell, idx ->
        [row.getCell(1).stringCellValue, header[idx - 2], (cell.cellTypeEnum in  [CellType.NUMERIC]) ? cell.numericCellValue : cell.stringCellValue]
      }
    }.inject([]){merge, item -> merge + item}
  }
}

def split_cols(dataset) { 
  dataset.collect { bottler_channel, kpi_date, score ->
    def (bottler, channel) = bottler_channel.split("_", 2)
    def (kpi, date) = kpi_date.split("_")
    [Date.parse( 'MMMyy', date ).format('YYYY-MM') , bottler, channel, kpi, score] 
  }
}

def cal_expr(dataset) {
    dataset
}

def cocacola_xls = "/home/spiderdt/work/git/spiderdt-working/larluo/score.xlsm"
cocacola_xls.with{ parse_xls(it, "score") }.with{ split_cols(it) }.with{ cal_expr(it) }.take(10).each{println(it)}
