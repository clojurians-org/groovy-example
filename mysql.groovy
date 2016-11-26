@Grab('mysql:mysql-connector-java:6.0.3')

import groovy.sql.Sql
import com.mysql.cj.jdbc.MysqlDataSource

def sql = new Sql(new MysqlDataSource().each {
              it.url = 'jdbc:mysql://192.168.1.2:3306/state_store?useSSL=false'
              it.user = 'state_store'
              it.password = 'spiderdt'
          })


println(sql)


sql.close()
