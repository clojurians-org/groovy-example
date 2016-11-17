package proto.hadoop
import com.google.protobuf.ByteString
import org.spiderdt.hadoop.ipc.protobuf.RpcHeaderProtos
import org.spiderdt.hadoop.ipc.protobuf.IpcConnectionContextProtos
import org.spiderdt.hadoop.ipc.protobuf.ProtobufRpcEngineProtos
import org.spiderdt.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos
import org.spiderdt.hadoop.hdfs.protocol.proto.HdfsProtos
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.io.DataInputStream
import java.util.Date
import java.text.SimpleDateFormat


class ProtoServiceClient {
    static final RpcConstants_DUMMY_CLIENT_ID = ByteString.copyFrom("".getBytes())
    static final RpcConstants_CONNECTION_CONTEX_CALL_ID = -3
    
    static final call_header = RpcHeaderProtos.RpcRequestHeaderProto.newBuilder().each {
                          it.rpcOp = RpcHeaderProtos.RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET
                          it.rpcKind = RpcHeaderProtos.RpcKindProto.RPC_PROTOCOL_BUFFER
                          it.callId = 0 // NORMAL
                          it.clientId = RpcConstants_DUMMY_CLIENT_ID
                      }.build()
    
    static writeHeaderAndContext(hdfsClient) {
        def header =RpcHeaderProtos. RpcRequestHeaderProto.newBuilder().each {
                         it.rpcOp = RpcHeaderProtos.RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET
                         it.rpcKind = RpcHeaderProtos.RpcKindProto.RPC_PROTOCOL_BUFFER
                         it.callId = RpcConstants_CONNECTION_CONTEX_CALL_ID
                         it.clientId = RpcConstants_DUMMY_CLIENT_ID
                     }.build()
        def context = IpcConnectionContextProtos.IpcConnectionContextProto.newBuilder().each {
                         it.protocol = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
                         it.userInfo = IpcConnectionContextProtos.UserInformationProto.newBuilder().each {
                         it.effectiveUser = "spiderdt"
                                       }.build()
                      }.build()
        def bos = new ByteArrayOutputStream().each {
            header.writeDelimitedTo(it)
            context.writeDelimitedTo(it)
        }
        hdfsClient.socket_out.writeInt(bos.size())
        bos.writeTo(hdfsClient.socket_out)
    }
    static openHadoopClient(hostname, port, username, password) {
        def RpcConstants_HEADER = "hrpc"
        def RpcConstants_CURRENT_VERSION = 9
        def PRC_PRC_SERVICE_CLASS_DEFAULT = 0
        def Server_AuthProtocol = 0 // NONE(0), SASL(-33)
        def socket = javax.net.SocketFactory.getDefault().createSocket().each {
                         it.keepAlive = true
                         it.connect(new java.net.InetSocketAddress(hostname, port))
                         it.soTimeout = 0
                     }
        def socket_in =  new DataInputStream(socket.getInputStream())
        def socket_out = new DataOutputStream(socket.getOutputStream())
        socket_out.writeBytes(RpcConstants_HEADER)
        socket_out.writeByte(RpcConstants_CURRENT_VERSION)
        socket_out.writeByte(PRC_PRC_SERVICE_CLASS_DEFAULT)
        socket_out.writeByte(Server_AuthProtocol)
    
        [socket: socket, socket_in: socket_in, socket_out: socket_out, args: [hostname: hostname, port: port, username: username, password: password]]
    }
    static refreshHdfsClient(_hdfsClient) {
        def args = _hdfsClient.args
        if(!_hdfsClient.connected) {
            System.out.println("reconnecting...")
            _hdfsClient.putAll(openHdfsClient(args.hostname, args.port, args.username, args.password))
        }
        _hdfsClient
    }
   static getListing(_hdfsClient, src, startAfter = "", needLocation = false) {
        def hdfsClient = refreshHdfsClient(_hdfsClient)
        writeHeaderAndContext(hdfsClient)
        def call_method = ProtobufRpcEngineProtos.RequestHeaderProto.newBuilder().each {
                              it.methodName = "getListing"
                              it.declaringClassProtocolName = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
                              it.clientProtocolVersion = 1
                          }.build()
        def call_args = ClientNamenodeProtocolProtos.GetListingRequestProto.newBuilder().each {
                            it.src = src
                            it.startAfter = ByteString.copyFrom(startAfter.getBytes())
                            it.needLocation = needLocation
                        }.build()
    
    
        def bos = new ByteArrayOutputStream().each {
                      call_header.writeDelimitedTo(it)
                      call_method.writeDelimitedTo(it)
                      call_args.writeDelimitedTo(it)
        }
    
        hdfsClient.socket_out.writeInt(bos.size())
        bos.writeTo(hdfsClient.socket_out)
        hdfsClient.socket_out.flush()
    
        hdfsClient.socket_in.readInt()
        def header_resp = RpcHeaderProtos.RpcResponseHeaderProto.parseDelimitedFrom(hdfsClient.socket_in)
        def sdf = new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:ss")
        def call_resp =  ClientNamenodeProtocolProtos.GetListingResponseProto.parseDelimitedFrom(hdfsClient.socket_in).dirList.partialListingList.collect {
                             [path:it.path.toStringUtf8(),
                              filePath:it.fileType,
                              length:it.length,
                              modification_time:sdf.format(new Date(it.modificationTime)),
                              access_time:sdf.format(new Date(it.accessTime))]
                         }
    }
    
    static create(_hdfsClient, src, createParent=true, createFlag='CREATE', perm=0644, clientName="", replication=1 ,blockSize=128*1024*1024) {
        def hdfsClient = refreshHdfsClient(_hdfsClient)
        writeHeaderAndContext(hdfsClient)
        def call_method = ProtobufRpcEngineProtos.RequestHeaderProto.newBuilder().each {
                              it.methodName = "create"
                              it.declaringClassProtocolName = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
                              it.clientProtocolVersion = 1
                          }.build()
        def call_args = ClientNamenodeProtocolProtos.CreateRequestProto.newBuilder().each {
                            it.src = src
                            it.masked = HdfsProtos.FsPermissionProto.newBuilder().each{ it.perm = perm }.build()
                            it.clientName = clientName
                            it.createFlag = ClientNamenodeProtocolProtos.CreateFlagProto[createFlag.toUpperCase()].number
                            it.createParent = createParent
                            it.replication = replication
                            it.blockSize = blockSize
                        }.build()
    
        def bos = new ByteArrayOutputStream().each {
                      call_header.writeDelimitedTo(it)
                      call_method.writeDelimitedTo(it)
                      call_args.writeDelimitedTo(it)
                 }
    
        hdfsClient.socket_out.writeInt(bos.size())
        bos.writeTo(hdfsClient.socket_out)
        hdfsClient.socket_out.flush()
    
        hdfsClient.socket_in.readInt()
        def header_resp = RpcHeaderProtos.RpcResponseHeaderProto.parseDelimitedFrom(hdfsClient.socket_in)
        def call_resp =  ClientNamenodeProtocolProtos.CreateResponseProto.parseDelimitedFrom(hdfsClient.socket_in)
    }
    
    
    static delete(_hdfsClient, src, recursive=false) {
        def hdfsClient = refreshHdfsClient(_hdfsClient)
        writeHeaderAndContext(hdfsClient)
        def call_method = ProtobufRpcEngineProtos.RequestHeaderProto.newBuilder().each {
                              it.methodName = "delete"
                              it.declaringClassProtocolName = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
                              it.clientProtocolVersion = 1
                          }.build()
        def call_args = ClientNamenodeProtocolProtos.DeleteRequestProto.newBuilder().each {
                            it.src = src
                            it.recursive = recursive
                        }.build()
        println(call_args)
    
        def bos = new ByteArrayOutputStream().each {
                      call_header.writeDelimitedTo(it)
                      call_method.writeDelimitedTo(it)
                      call_args.writeDelimitedTo(it)
        }
    
        hdfsClient.socket_out.writeInt(bos.size())
        bos.writeTo(hdfsClient.socket_out)
        hdfsClient.socket_out.flush()
    
        hdfsClient.socket_in.readInt()
        def header_resp = RpcHeaderProtos.RpcResponseHeaderProto.parseDelimitedFrom(hdfsClient.socket_in)
        println(header_resp)
        def call_resp =  ClientNamenodeProtocolProtos.DeleteResponseProto.parseDelimitedFrom(hdfsClient.socket_in)
    }
    
    static rename2(_hdfsClient, src, dst,overwriteDest=false, moveToTrash=false) {
        def hdfsClient = refreshHdfsClient(_hdfsClient)
        writeHeaderAndContext(hdfsClient)
    
        def call_method = ProtobufRpcEngineProtos.RequestHeaderProto.newBuilder().each {
                              it.methodName = "rename2"
                              it.declaringClassProtocolName = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
                              it.clientProtocolVersion = 1
                          }.build()
        def call_args = ClientNamenodeProtocolProtos.Rename2RequestProto.newBuilder().each {
                            it.src = src
                            it.dst = dst
                            it.overwriteDest = overwriteDest
                            it.moveToTrash = moveToTrash
                        }.build()
        println(call_args)
    
        def bos = new ByteArrayOutputStream().each {
                      call_header.writeDelimitedTo(it)
                      call_method.writeDelimitedTo(it)
                      call_args.writeDelimitedTo(it)
                  }
    
        hdfsClient.socket_out.writeInt(bos.size())
        bos.writeTo(hdfsClient.socket_out)
        hdfsClient.socket_out.flush()
    
        hdfsClient.socket_in.readInt()
        def header_resp = RpcHeaderProtos.RpcResponseHeaderProto.parseDelimitedFrom(hdfsClient.socket_in)
        println(header_resp)
        def call_resp =  ClientNamenodeProtocolProtos.Rename2ResponseProto.parseDelimitedFrom(hdfsClient.socket_in)
    
    }
    
    static mkdirs(_hdfsClient, src, perm=0644,createParent=false) {
        def hdfsClient = refreshHdfsClient(_hdfsClient)
        writeHeaderAndContext(hdfsClient)
        def call_method = ProtobufRpcEngineProtos.RequestHeaderProto.newBuilder().each {
                              it.methodName = "mkdirs"
                              it.declaringClassProtocolName = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
                              it.clientProtocolVersion = 1
                          }.build()
        def call_args = ClientNamenodeProtocolProtos.MkdirsRequestProto.newBuilder().each {
                            it.src = src
                            it.masked = HdfsProtos.FsPermissionProto.newBuilder().each{ it.perm = perm }.build()
                            it.createParent = createParent
                        }.build()
        println(call_args)
    
        def bos = new ByteArrayOutputStream().each {
                      call_header.writeDelimitedTo(it)
                      call_method.writeDelimitedTo(it)
                      call_args.writeDelimitedTo(it)
                  }
    
        hdfsClient.socket_out.writeInt(bos.size())
        bos.writeTo(hdfsClient.socket_out)
        hdfsClient.socket_out.flush()
    
        hdfsClient.socket_in.readInt()
        def header_resp = RpcHeaderProtos.RpcResponseHeaderProto.parseDelimitedFrom(hdfsClient.socket_in)
        println(header_resp)
        def call_resp =  ClientNamenodeProtocolProtos.MkdirsResponseProto.parseDelimitedFrom(hdfsClient.socket_in)
    
    }
    
    static getBlockLocations(_hdfsClient, src, offset=0,length=100) {
        def hdfsClient = refreshHdfsClient(_hdfsClient)
        writeHeaderAndContext(hdfsClient)
        def call_method = ProtobufRpcEngineProtos.RequestHeaderProto.newBuilder().each {
                              it.methodName = "getBlockLocations"
                              it.declaringClassProtocolName = "org.apache.hadoop.hdfs.protocol.ClientProtocol"
                              it.clientProtocolVersion = 1
                          }.build()
        def call_args = ClientNamenodeProtocolProtos.GetBlockLocationsRequestProto.newBuilder().each {
                            it.src = src
                            it.offset = offset
                            it.length = length
                        }.build()
        println(call_args)
    
        def bos = new ByteArrayOutputStream().each {
                      call_header.writeDelimitedTo(it)
                      call_method.writeDelimitedTo(it)
                      call_args.writeDelimitedTo(it)
                  }
    
        hdfsClient.socket_out.writeInt(bos.size())
        bos.writeTo(hdfsClient.socket_out)
        hdfsClient.socket_out.flush()
    
        hdfsClient.socket_in.readInt()
        def header_resp = RpcHeaderProtos.RpcResponseHeaderProto.parseDelimitedFrom(hdfsClient.socket_in)
        def call_resp =  ClientNamenodeProtocolProtos.GetBlockLocationsResponseProto.parseDelimitedFrom(hdfsClient.socket_in).locations.with{
                             [fileLength:it.fileLength,
                              blocks: it.blocksList.collect {[
                                 poolId: it.b.poolId,
                                 blockId: it.b.blockId,
                                 numBytes: it.b.numBytes,
                                 locs: it.locsList*.id*.ipAddr,
                                 corrupt: it.corrupt ]},
                              underConstruction:it.underConstruction ]}
    
    }
}
