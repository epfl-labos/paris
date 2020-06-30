#include "groupservice/group_server.h"
#include "common/exceptions.h"
#include "common/sys_logger.h"
#include "rpc/rpc_server.h"
#include "rpc/rpc_id.h"
#include <unistd.h>

namespace scc {

    GroupServer::GroupServer(unsigned short port, int numPartitions, int numDCs, int replicationFactor)
            : _serverPort(port),
              _numPartitions(numPartitions),
              _numDataCenters(numDCs),
              _partitionReplicationFactor(replicationFactor),
              _totalNumPartitions(numPartitions * replicationFactor),
              _numRegisteredPartitions(0),
              _allPartitionsRegistered(false),
              _numReadyPartitions(0),
              _allPartitionsReady(false) {

        fprintf(stdout, "Constructing GroupServer!\n");


        _registeredPartitions.resize(_numPartitions);
        _registeredPartitionsPresenceMap.resize(numPartitions);
        for (int i = 0; i < _numPartitions; i++) {
            _registeredPartitions[i].resize(_numDataCenters);
            _registeredPartitionsPresenceMap[i].resize(_numDataCenters);
        }

        for (int i = 0; i < _numPartitions; i++) {
            for (int j = 0; j < _numDataCenters; j++) {
                _registeredPartitionsPresenceMap[i][j] = false;

            }
        }


    }

    void GroupServer::Run() {
        fprintf(stdout, "GroupServer:RUN()\n");
        ServeConnection();
    }

    void GroupServer::ServeConnection() {
        try {
            TCPServerSocket serverSocket(_serverPort);
            while (true) {
                TCPSocket *clientSocket = serverSocket.accept();
                std::thread t(&GroupServer::HandleRequest, this, clientSocket);
                t.detach();
            }
        } catch (SocketException &e) {
            fprintf(stdout, "Server socket exception: %s\n", e.what());
            fflush(stdout);
        }
    }

    int GroupServer::getMostConvenientRemoteReplicaIndex(int partitionId, int dcId) const {
        int index = (dcId + 1) % _numDataCenters;
        while (_registeredPartitionsPresenceMap[partitionId][index] == false) {
            index = (index + 1) % _numDataCenters;
        }
        return index;
    }

    int GroupServer::getLeastUsedRemoteReplicaIndex(int partitionId, int dcId) {
        int index;

        ReplicaUsageQueue *replicasQ = pidToReplicasQ[partitionId];
        ReplicaNode topReplica = std::move(const_cast<ReplicaNode &>(replicasQ->top()));
        replicasQ->pop();
        index = topReplica.getReplicaId();
        topReplica.increaseRefCounter();
        replicasQ->push(topReplica);

        return index;
    }

    void GroupServer::fillInMissingPartitions() {

        //assign node IDs to partitions
        int nId = 0;
        for (int i = 0; i < _numPartitions; i++) {
            ReplicaUsageQueue *pidQueue = new ReplicaUsageQueue;

            for (int j = 0; j < _numDataCenters; j++) {
                if (_registeredPartitions[i][j].Name != "NULL") {
                    _registeredPartitionsPresenceMap[i][j] = true;
                    _registeredPartitions[i][j].NodeId = nId++;
                    _registeredPartitions[i][j].isLocal = true;
                    ReplicaNode rep(j);
                    pidQueue->push(rep);
                }
            }
            pidToReplicasQ[i] = pidQueue;
        }

//        printPartitionMaps();


        for (int i = 0; i < _numPartitions; i++) {
            for (int j = 0; j < _numDataCenters; j++) {

                if (_registeredPartitionsPresenceMap[i][j] == false) {

//                    int replicaID = getMostConvenientRemoteReplicaIndex(i, j);
                    int replicaID = getLeastUsedRemoteReplicaIndex(i, j);


                    for (int p = 0; p < _numPartitions; p++) {
                        if (_registeredPartitionsPresenceMap[p][j] != false)
                            _registeredPartitions[i][replicaID].adopteeNodesIndexes.push_back(std::make_pair(p, j));
                    }

                    _registeredPartitions[i][j] = _registeredPartitions[i][replicaID];
                    _registeredPartitions[i][j].isLocal = false;

                }
            }
        }

    }

    void GroupServer::printRegisteredPartitions() {

        std::string result;
        for (int i = 0; i < _numPartitions; i++) {
            for (int j = 0; j < _numDataCenters; j++) {
                result += (boost::format("[%d][%d] ") % i % j).str() + _registeredPartitions[i][j].toString();

            }
        }
        SLOG(result);
    }

    void GroupServer::printPartitionMaps() {

        std::string result;
        std::string entry;
        for (int i = 0; i < _numPartitions; i++) {
            for (int j = 0; j < _numDataCenters; j++) {
                if (_registeredPartitionsPresenceMap[i][j])
                    entry = "TRUE";
                else entry = "FALSE";
                result +=
                        (boost::format("[%d][%d] = %s ") % i % j % entry).str();

            }
        }
        SLOG(result);
    }


    void GroupServer::HandleRequest(TCPSocket *clientSocket) {
        try {
            RPCServer rpcServer(clientSocket);
            while (true) {
                // get rpc request
                PbRpcRequest rpcRequest;
                PbRpcReply rpcReply;
                rpcServer.RecvRequest(rpcRequest);

                switch (static_cast<RPCMethod> (rpcRequest.methodid())) {
                    case RPCMethod::EchoTest: {
                        // get arguments
                        PbRpcEchoTest echoRequest;
                        PbRpcEchoTest echoReply;
                        echoRequest.ParseFromString(rpcRequest.arguments());
                        echoReply.set_text(echoRequest.text());
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(echoReply.SerializeAsString());
                        rpcServer.SendReply(rpcReply);
                        break;
                    }
                    case RPCMethod::RegisterPartition: {
                        // get op argument
                        PbPartition arg;
                        arg.ParseFromString(rpcRequest.arguments());
                        PbRpcGroupServiceResult result;
                        // execute op
                        DBPartition p(arg.name(), arg.publicport(),
                                      arg.partitionport(), arg.replicationport(),
                                      arg.partitionid(), arg.replicaid());


                        int nodeId = -1;
                        {
                            std::lock_guard<std::mutex> lk(_registrationMutex);

                            _registeredPartitions[p.PartitionId][p.ReplicaId] = p;
                            _numRegisteredPartitions += 1;


                            SLOG((boost::format("Partition %d:%d at %s:%d registered.")
                                  % p.PartitionId % p.ReplicaId % p.Name % p.PublicPort).str());

                            if (_numRegisteredPartitions == _totalNumPartitions) {
                                //fill the missing parts

                                fillInMissingPartitions();
//                                printRegisteredPartitions();

                                _allPartitionsRegistered = true;
                                SLOG("All partitions registered.");
                            }
                        }

                        result.set_succeeded(true);
                        result.set_nodeid(nodeId);
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(result.SerializeAsString());
                        rpcServer.SendReply(rpcReply);
                        break;
                    }
                    case RPCMethod::GetRegisteredPartitions: {
                        // wait until all partitions registered
                        while (!_allPartitionsRegistered) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(100));
                        }

                        PbRegisteredPartitions opResult;
                        // execute op
                        opResult.set_numpartitions(_numPartitions);
                        opResult.set_numreplicasperpartition(_numDataCenters);

                        for (int i = 0; i < _numPartitions; i++) {
                            for (int j = 0; j < _numDataCenters; j++) {
                                DBPartition &p = _registeredPartitions[i][j];
                                PbPartition *rp = opResult.add_partition();
                                rp->set_name(p.Name);
                                rp->set_publicport(p.PublicPort);
                                rp->set_partitionport(p.PartitionPort);
                                rp->set_replicationport(p.ReplicationPort);
                                rp->set_partitionid(p.PartitionId);
                                rp->set_replicaid(p.ReplicaId);
                                rp->set_nodeid(p.NodeId);
                                rp->set_islocal(p.isLocal);
                            }
                        }
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer.SendReply(rpcReply);
                        break;
                    }
                    case RPCMethod::GetReplicationFactor: {
                        while (!_allPartitionsRegistered) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(100));
                        }
                        PbReplicationFactor opResult;


                        opResult.set_replicationfactor(_partitionReplicationFactor);

                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer.SendReply(rpcReply);
                        break;
                    }

                    case RPCMethod::GetPartitionUsersIds: {
                        // wait until all partitions registered
                        while (!_allPartitionsRegistered) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(100));
                        }

                        PbServerCoordinates arg;
                        arg.ParseFromString(rpcRequest.arguments());

                        assert(arg.has_partitionid() && arg.has_replicaid());
                        PbPartitionUsersIds opResult;

#ifdef DEBUG_BASIC_PARTIAL_REPLICATION
                        SLOG((boost::format("RPCMethod::GetPartitionUsersIds arg.partitionid() =%d arg.replicaid() =%d")
                              % arg.partitionid() % arg.replicaid()).str());
#endif
                        // execute op
                        DBPartition &p = _registeredPartitions[arg.partitionid()][arg.replicaid()];
                        for (std::pair<int, int> const &coord: p.adopteeNodesIndexes) {

                            PbServerCoordinates *servCoord = opResult.add_coordinates();

#ifdef DEBUG_BASIC_PARTIAL_REPLICATION
                            SLOG((boost::format("PbServerCoordinates.first =%d PbServerCoordinates.second =%d")
                                  % coord.first % coord.second).str());
#endif

                            servCoord->set_partitionid(coord.first);
                            servCoord->set_replicaid(coord.second);
                        }
//
//                        SLOG((boost::format("REMOTE_USAGES.size()=%d for partitionid=%d replicaid=%d")
//                              % p.adopteeNodesIndexes.size() % p.PartitionId % p.ReplicaId).str());

                        opResult.set_numusages(p.adopteeNodesIndexes.size());

                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer.SendReply(rpcReply);

                        break;
                    }
                    case RPCMethod::NotifyReadiness: {
                        // get op argument
                        PbPartition arg;
                        arg.ParseFromString(rpcRequest.arguments());
                        PbRpcGroupServiceResult result;

                        // execute op
                        DBPartition p(arg.name(), arg.publicport(),
                                      arg.partitionport(), arg.replicationport(),
                                      arg.partitionid(), arg.replicaid());
                        {
                            std::lock_guard<std::mutex> lk(_readinessMutex);

                            _numReadyPartitions++;
                            SLOG((boost::format("Partition %d:%d at %s:%d ready.")
                                  % p.PartitionId % p.ReplicaId % p.Name % p.PublicPort).str());

                            if (_numReadyPartitions == _totalNumPartitions) {
                                _allPartitionsReady = true;
                                SLOG("All partitions are ready to serve client requests.");
                            }
                        }
                        result.set_succeeded(true);

                        // wait until all partitions ready
                        while (!_allPartitionsReady) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(100));
                        }

                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(result.SerializeAsString());
                        rpcServer.SendReply(rpcReply);
                        break;
                    }
                    default:
                        throw KVOperationException("Unsupported operation.");
                }
            }
        } catch (SocketException &e) {
            fprintf(stdout, "GroupServer:Client serving thread socket exception: %s\n", e.what());
            fflush(stdout);
        }
    }

}
