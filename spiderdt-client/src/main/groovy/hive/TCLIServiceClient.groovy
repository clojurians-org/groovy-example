package thrift.hive
import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.TSaslClientTransport
import org.apache.thrift.protocol.TBinaryProtocol

import org.spiderdt.hive.service.rpc.thrift.*

import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.PasswordCallback
import javax.security.auth.callback.UnsupportedCallbackException

class TCLIServiceClient {
    static openHiveClient(hostname, port, username, password) {
        def transport = new TSaslClientTransport (
            'PLAIN',
             null,
             null,
             null,
             [:],
             { it.each {
                 if (it instanceof NameCallback)  it.setName(username)
                 else if (it instanceof PasswordCallback) it.setPassword(password.toCharArray())
                 else throw new UnsupportedCallbackException(it)
             }},
             new TSocket(hostname, port)
        )
        transport.open()
        def client = new TCLIService.Client(new TBinaryProtocol(transport))
        def sessionHandle = client.OpenSession(new TOpenSessionReq(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8)).sessionHandle
         [client: client, sessionHandle: sessionHandle]
    }
    
    static getSchemas(hiveClient) {
        def op_handle = hiveClient.client.GetSchemas(new TGetSchemasReq(hiveClient.sessionHandle)).operationHandle
        def result = hiveClient.client.FetchResults(new TFetchResultsReq(op_handle, TFetchOrientation.FETCH_NEXT, 100)).results.columns[0].fieldValue.values
        hiveClient.client.CloseOperation(new TCloseOperationReq(op_handle))
        result
    }
    
    static getTables(hiveClient, schemaId) {
        def op_handle = hiveClient.client.GetTables(new TGetTablesReq(hiveClient.sessionHandle).each {it.schemaName = schemaId}).operationHandle
        def result = hiveClient.client.FetchResults(new TFetchResultsReq(op_handle, TFetchOrientation.FETCH_NEXT, 100)).results.columns[2].fieldValue.values
        hiveClient.client.CloseOperation(new TCloseOperationReq(op_handle))
        result
    }
    
    static executeStatement(hiveClient, sql) {
        def op_handle = hiveClient.client.ExecuteStatement(new TExecuteStatementReq(hiveClient.sessionHandle, sql)).operationHandle
    }
    
    static getColumns(hiveClient, schemaId, tableId) {
        def sql = "SELECT * FROM ${schemaId}.${tableId}".toString()
        def op_handle = executeStatement(hiveClient, sql) // parameter inside sql
        def result = hiveClient.client.GetResultSetMetadata(new TGetResultSetMetadataReq(op_handle)).schema.columns.collect{
            [columnName: it.columnName, type: it.typeDesc.types[0].fieldValue.type, comment:it.comment]
        }
        // [columnName, type, comment]
        hiveClient.client.CloseOperation(new TCloseOperationReq(op_handle))
        result
    }
    
    static getDataFrame(hiveClient, schemaId, tableId) {
        def sql = "SELECT * FROM ${schemaId}.${tableId}".toString()
        def op_handle = executeStatement(hiveClient, sql) // parameter inside sql
        def header=  hiveClient.client.GetResultSetMetadata(new TGetResultSetMetadataReq(op_handle)).schema.columns.collect{it.columnName}
        def data= hiveClient.client.FetchResults(new TFetchResultsReq(op_handle, TFetchOrientation.FETCH_NEXT, 100)).results.columns.collect{it.fieldValue.values}
        def result = [header: header, data: data]
        hiveClient.client.CloseOperation(new TCloseOperationReq(op_handle))
        result
    }
}
