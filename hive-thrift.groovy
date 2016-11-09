#! /bin/groovy -Dgroovy.grape.report.downloads=true -Divy.message.logger.level=4
@Grab('org.apache.thrift:libthrift:0.9.2')
@Grab('com.spiderdt.framework:spiderdt-client:0.1')
@Grab(group='org.slf4j', module='slf4j-api', version='1.7.7')
 
import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.TSaslClientTransport
import org.apache.thrift.protocol.TBinaryProtocol

import org.spiderdt.hive.service.rpc.thrift.TProtocolVersion
import org.spiderdt.hive.service.rpc.thrift.TCLIService
import org.spiderdt.hive.service.rpc.thrift.TCLIService
import org.spiderdt.hive.service.rpc.thrift.TOpenSessionReq
import org.spiderdt.hive.service.rpc.thrift.TExecuteStatementReq
import org.spiderdt.hive.service.rpc.thrift.TGetOperationStatusReq
import org.spiderdt.hive.service.rpc.thrift.TFetchResultsReq
import org.spiderdt.hive.service.rpc.thrift.TFetchOrientation
import org.spiderdt.hive.service.rpc.thrift.TGetTypeInfoReq
import org.spiderdt.hive.service.rpc.thrift.TGetResultSetMetadataReq

import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.PasswordCallback
import javax.security.auth.callback.UnsupportedCallbackException

//println("hello world")

      def transport = new TSaslClientTransport (
            'PLAIN',
            null,
            null,
            null,
            [:],
            { it.each {
                if (it instanceof NameCallback)  it.setName("spiderdt")
                else if (it instanceof PasswordCallback) it.setPassword("spiderdt".toCharArray()) 
                else throw new UnsupportedCallbackException(it) 
            }},
            new TSocket("192.168.1.3", 10000)
      )
      transport.open()
      def client = new TCLIService.Client(new TBinaryProtocol(transport))
      def sess_handle = client.OpenSession(new TOpenSessionReq(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)).sessionHandle

      def sql = "select * from model.d_bolome_product_category limit 10 "
      def op_handle = client.ExecuteStatement(new TExecuteStatementReq(sess_handle, sql)).operationHandle
      // def op_status = client.GetOperationStatus(new TGetOperationStatusReq(op_handle)).status.statusCode
      // def op_rowset = client.FetchResults(new TFetchResultsReq(op_handle, TFetchOrientation.FETCH_NEXT, 100)).results
      def op_metadata = client.GetResultSetMetadata(new TGetResultSetMetadataReq(op_handle)).schema

      transport.close()
      // println( op_rowset.properties )
      println (op_metadata.properties)
