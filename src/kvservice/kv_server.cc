#include "kvservice/kv_server.h"

namespace scc {

    KVTxServer::KVTxServer(std::string name, unsigned short publicPort, int totalNumKeys) {
        //_serverName = Utils::GetHostName();
        _serverName = name;
        _publicPort = publicPort;
        _coordinator = new CoordinatorTx(_serverName, publicPort, totalNumKeys);
    }

    KVTxServer::KVTxServer(std::string name,
                           unsigned short publicPort,
                           unsigned short partitionPort,
                           unsigned short replicationPort,
                           int partitionId,
                           int replicaId,
                           int totalNumKeys,
                           std::string groupServerName,
                           int groupServerPort,
                           int replicationFactor) {
        _serverName = name;
        _publicPort = publicPort;
        _partitionPort = partitionPort;
        _replicationPort = replicationPort;
        _partitionId = partitionId;
        _replicaId = replicaId;
        _coordinator = new CoordinatorTx(_serverName,
                                         _publicPort,
                                         _partitionPort,
                                         _replicationPort,
                                         partitionId,
                                         replicaId,
                                         totalNumKeys,
                                         groupServerName,
                                         groupServerPort,
                                         replicationFactor);
    }

    KVTxServer::~KVTxServer() {
        delete _coordinator;
    }

    void KVTxServer::Run() {
        // partitions of the same replication group
        std::thread tPartition(&KVTxServer::ServePartitionConnection, this);
        tPartition.detach();

        std::thread tPartitionW(&KVTxServer::ServePartitionsWriteConnection, this);
        tPartitionW.detach();

        // replicas of the same partition
        std::thread tReplication(&KVTxServer::ServeReplicationConnection, this);
        tReplication.detach();

        // initialize
        _coordinator->Initialize();

        // serve operation request from clients
        ServePublicConnection();
    }

    void KVTxServer::ServePublicConnection() {
        try {
            TCPServerSocket serverSocket(_publicPort);
            for (;;) {
                TCPSocket *clientSocket = serverSocket.accept();
                std::thread t(&KVTxServer::HandlePublicRequest, this, clientSocket);
                t.detach();
            }
        } catch (SocketException &e) {
            fprintf(stdout, "Server public socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "[SEVERE FAILURE ServePublicConnection] Server public socket exception: %s\n", "...");
            fflush(stdout);
        }
    }

    void KVTxServer::HandlePublicRequest(TCPSocket *clientSocket) {
        try {
            RPCServerPtr rpcServer(new RPCServer(clientSocket));
            for (;;) {

                // get rpc request
                PbRpcRequest rpcRequest;
                PbRpcReply rpcReply;
                rpcServer->RecvRequest(rpcRequest);

                switch (static_cast<RPCMethod> (rpcRequest.methodid())) {
                    ////////////////////////////////
                    // key-value store interfaces //
                    ////////////////////////////////

                    case RPCMethod::GetServerConfig: {
                        PbRpcTxPublicGetServerConfigResult opResult;
                        opResult.set_succeeded(true);

                        opResult.set_numpartitions(_coordinator->NumPartitions());
                        opResult.set_numdatacenters(_coordinator->NumDataCenters());
                        opResult.set_partitionreplicationfactor(_coordinator->PartitionReplicationFactor());
                        opResult.set_replicaid(_coordinator->ReplicaId());

                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);
                        break;
                    }

                    case RPCMethod::EchoTest: {
                        // get arguments
                        PbRpcEchoTest echoRequest;
                        PbRpcEchoTest echoReply;
                        echoRequest.ParseFromString(rpcRequest.arguments());
                        echoReply.set_text(echoRequest.text());
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(echoReply.SerializeAsString());
                        rpcServer->SendReply(rpcReply);
                        break;
                    }

                    case RPCMethod::TxStart: {

                        PbRpcPublicStartArg opArg;
                        PbRpcPublicStartResult opResult;

                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleTxStart<PbRpcPublicStartArg, PbRpcPublicStartResult>(opArg, opResult);

                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);

#ifdef MEASURE_STATISTICS
                        SysStats::NumPublicTxStartRequests++;
#endif

                        break;
                    }

                    case RPCMethod::TxRead: {

                        PbRpcPublicReadArg opArg;
                        PbRpcPublicReadResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleTxRead<PbRpcPublicReadArg, PbRpcPublicReadResult>(opArg, opResult);

                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);

#ifdef MEASURE_STATISTICS
                        SysStats::NumPublicTxReadRequests++;
#endif
                        break;
                    }

                    case RPCMethod::TxCommit: {
                        PbRpcPublicCommitArg opArg;
                        PbRpcPublicCommitResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleTxCommit<PbRpcPublicCommitArg, PbRpcPublicCommitResult>(opArg, opResult);

                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);

#ifdef MEASURE_STATISTICS
                        SysStats::NumPublicTxCommitRequests++;
#endif

                        break;
                    }

                    case RPCMethod::ShowItem: {
                        // get arguments
                        PbRpcKVPublicShowArg opArg;
                        PbRpcKVPublicShowResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());
                        // execute op
                        std::string itemVersions;
                        bool ret = _coordinator->ShowItem(opArg.key(), itemVersions);
                        opResult.set_succeeded(ret);
                        if (ret) {
                            opResult.set_returnstring(itemVersions);
                        }
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);
                        break;
                    }

                    case RPCMethod::ShowDB: {
                        // get arguments
                        PbRpcKVPublicShowResult opResult;
                        // execute op
                        std::string allItemVersions;
                        bool ret = _coordinator->ShowDB(allItemVersions);
                        opResult.set_succeeded(ret);
                        if (ret) {
                            opResult.set_returnstring(allItemVersions);
                        }
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);
                        break;
                    }

                    case RPCMethod::ShowState: {
                        // get arguments
                        PbRpcKVPublicShowResult opResult;
                        // execute op
                        std::string stateStr = (boost::format("*%s|") % _serverName).str();
                        bool ret = _coordinator->ShowState(stateStr);
                        opResult.set_succeeded(ret);
                        if (ret) {

                            opResult.set_returnstring(stateStr);
                        }
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);

                        break;
                    }

                    case RPCMethod::ShowStateCSV: {
                        // get arguments
                        PbRpcKVPublicShowResult opResult;
                        // execute op
                        std::string stateStr = (boost::format("%s") % _serverName).str();
                        bool ret = _coordinator->ShowStateCSV(stateStr);
                        opResult.set_succeeded(ret);
                        if (ret) {

                            opResult.set_returnstring(stateStr);
                        }
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);

                        break;
                    }

                    case RPCMethod::DumpLatencyMeasurement: {
                        // get arguments
                        PbRpcKVPublicShowResult opResult;
                        // execute op
                        std::string resultStr;
                        bool ret = _coordinator->DumpLatencyMeasurement(resultStr);
                        opResult.set_succeeded(ret);
                        if (ret) {
                            opResult.set_returnstring(resultStr);
                        }
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);
                        break;
                    }

                    case RPCMethod::GetRemotePartitionsIds: {

                        // get arguments
                        PbRpcPublicGetRemotePartitionsIdsResult opResult;
                        // execute op
                        std::string resultStr;
                        std::vector<int> ids = _coordinator->getRemotePartitionsIds();
                        opResult.set_succeeded(true);

                        for (unsigned int i = 0; i < ids.size(); ++i) {
                            opResult.add_pids(ids[i]);
                        }

                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);
                        break;
                    }

                    default:
                        throw KVOperationException("(public) Unsupported operation.");
                }
            }
        } catch (SocketException &e) {

            fprintf(stdout, "HandlePublicRequest:Client serving thread socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "SEVERE Server public socket exception: %s\n", "...");
            fflush(stdout);
        }
    }

    template<class Argument, class Result>
    void KVTxServer::HandleTxStart(Argument &opArg, Result &opResult) {
        TxConsistencyMetadata cdata;

        //Process the received arguments

#ifndef EVENTUAL_CONSISTENCY

        const PbPhysicalTimeSpec &v = opArg.ust();
        cdata.UST.Seconds = v.seconds();
        cdata.UST.NanoSeconds = v.nanoseconds();

#endif //EVENTUAL_CONSISTENCY

        bool ret = _coordinator->TxStart(cdata);

        opResult.set_succeeded(ret);

        if (ret) {
            opResult.set_id(cdata.txId);

#ifndef EVENTUAL_CONSISTENCY
            opResult.mutable_ust()->set_seconds(cdata.UST.Seconds);
            opResult.mutable_ust()->set_nanoseconds(cdata.UST.NanoSeconds);
#endif //EVENTUAL_CONSISTENCY

        }
    }


    template<class Argument, class Result>
    void KVTxServer::HandleTxRead(Argument &opArg, Result &opResult) {
        TxConsistencyMetadata cdata;
        std::vector<std::string> keySet;
        std::vector<std::string> valueSet;

        for (int i = 0; i < opArg.key_size(); ++i) {
            keySet.push_back(opArg.key(i));
        }

#ifdef NO_START_TX

        if (opArg.notxid()) {

            PhysicalTimeSpec dummyTime;
            cdata.txId = _coordinator->generateTxIDandAddTxInActiveTxs(dummyTime);
        } else {
            cdata.txId = opArg.id();
        }
#else

        cdata.txId = opArg.id();

#endif //NO_START_TX

        bool ret = _coordinator->TxRead(cdata.txId, keySet, valueSet);

        opResult.set_succeeded(ret);

        if (ret) {
            for (unsigned int i = 0; i < valueSet.size(); ++i) {
                opResult.add_value(valueSet[i]);
            }

        }

#ifdef NO_START_TX

        opResult.set_id(cdata.txId);

#endif //NO_START_TX


    }

    template<class Argument, class Result>
    void KVTxServer::HandleTxCommit(Argument &opArg, Result &opResult) {
        TxConsistencyMetadata cdata;
        std::vector<std::string> keySet;
        std::vector<std::string> valueSet;

        cdata.txId = opArg.id();

        /* A read-only transaction still needs to be committed in order to clean-up */
        if (opArg.key_size() != 0) {

            for (int i = 0; i < opArg.key_size(); ++i) {
                keySet.push_back(opArg.key(i));
                valueSet.push_back(opArg.value(i));
            }

            const PbPhysicalTimeSpec &v = opArg.lct();
            cdata.CT.Seconds = v.seconds();
            cdata.CT.NanoSeconds = v.nanoseconds();
        }

        bool ret = _coordinator->TxCommit(cdata, keySet, valueSet);

        assert(ret == true);

        opResult.set_succeeded(ret);

        if (ret) {
#ifdef DEBUG_MSGS
            SLOG((boost::format("TXID : %d committed with commitTime = %s") % cdata.txId %
                  Utils::physicaltime2str(cdata.CT).c_str()).str());
#endif
            opResult.mutable_ct()->set_seconds(cdata.CT.Seconds);
            opResult.mutable_ct()->set_nanoseconds(cdata.CT.NanoSeconds);
        } else {
            SLOG((boost::format("TXID : %d FAILED") % cdata.txId).str());
            throw KVOperationException("Transaction commit failed.");
        }
    }


    void KVTxServer::ServePartitionConnection() {
        try {
            TCPServerSocket serverSocket(_partitionPort);
            for (;;) {
                TCPSocket *clientSocket = serverSocket.accept();
                std::thread t(&KVTxServer::HandlePartitionRequest, this, clientSocket);
                t.detach();
            }
        } catch (SocketException &e) {
            fprintf(stdout, "Server partition socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "SEVERE Server partition socket exception: %s\n", "...");
            fflush(stdout);
        }
    }


    void KVTxServer::ServePartitionsWriteConnection() {
        try {
            TCPServerSocket serverSocket(_partitionPort + 100);
            for (;;) {
                TCPSocket *clientSocket = serverSocket.accept();
                std::thread t(&KVTxServer::HandlePartitionsWriteRequest, this, clientSocket);
                t.detach();
            }
        } catch (SocketException &e) {
            fprintf(stdout, "Server partition writes socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "SEVERE Server partition writes socket exception: %s\n", "...");
            fflush(stdout);
        }
    }


    void KVTxServer::HandlePartitionRequest(TCPSocket *clientSocket) {
        try {
            RPCServerPtr rpcServer(new RPCServer(clientSocket));
            DBPartition servedPartition;

            for (;;) {
                // get rpc request
                PbRpcRequest rpcRequest;
                PbRpcReply rpcReply;
                rpcServer->RecvRequest(rpcRequest);

                switch (static_cast<RPCMethod> (rpcRequest.methodid())) {

                    case RPCMethod::InternalShowItem: {
                        // get arguments
                        PbRpcKVInternalShowItemArg opArg;
                        PbRpcKVInternalShowItemResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());
                        // execute op
                        std::string itemVersions;
                        bool ret = _coordinator->ShowItem(opArg.key(), itemVersions);
                        opResult.set_succeeded(ret);
                        if (ret) {
                            opResult.set_itemversions(itemVersions);
                        }
                        // send rpc reply
                        rpcReply.set_msgid(rpcRequest.msgid());
                        rpcReply.set_results(opResult.SerializeAsString());
                        rpcServer->SendReply(rpcReply);
                        break;
                    }

                    case RPCMethod::InitializePartitioning: {
                        PbPartition opArg;
                        opArg.ParseFromString(rpcRequest.arguments());

                        servedPartition.Name = opArg.name();
                        servedPartition.PublicPort = opArg.publicport();
                        servedPartition.PartitionPort = opArg.partitionport();
                        servedPartition.ReplicationPort = opArg.replicationport();
                        servedPartition.PartitionId = opArg.partitionid();
                        servedPartition.ReplicaId = opArg.replicaid();
                        break;
                    }

                    case RPCMethod::InternalTxSliceReadKeys: {

                        PbRpcInternalTxSliceReadKeysArg opArg;
                        PbRpcInternalTxSliceReadKeysResult opResult;
                        opArg.ParseFromString(rpcRequest.arguments());

#ifdef MEASURE_STATISTICS
                        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
#endif

                        HandleInternalTxSliceReadKeys<PbRpcInternalTxSliceReadKeysArg, PbRpcInternalTxSliceReadKeysResult>(
                                opArg, opResult);

#ifdef MEASURE_STATISTICS
                        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                        double duration = (endTime - startTime).toMilliSeconds();
                        SysStats::CohordHandleInternalSliceReadLatencySum =
                                SysStats::CohordHandleInternalSliceReadLatencySum + duration;
                        SysStats::NumCohordHandleInternalSliceReadLatencyMeasurements++;
#endif

                        opResult.set_id(opArg.id());
                        opResult.set_srcpartition(_partitionId);
                        opResult.set_srcdatacenter(_replicaId);
                        int callingPartition = opArg.srcpartition();
                        int callingDatacenter = opArg.srcdatacenter();

#ifdef MEASURE_STATISTICS
                        startTime = Utils::GetCurrentClockTime();
#endif
                        _coordinator->template C_SendInternalTxSliceReadKeysResult<PbRpcInternalTxSliceReadKeysResult>(
                                opResult, callingPartition, callingDatacenter);

#ifdef MEASURE_STATISTICS
                        endTime = Utils::GetCurrentClockTime();
                        duration = (endTime - startTime).toMilliSeconds();
                        SysStats::CohordSendingReadResultsLatencySum =
                                SysStats::CohordSendingReadResultsLatencySum + duration;
                        SysStats::NumCohordSendingReadResultsLatencyMeasurements++;
#endif

                        break;
                    }


                    case RPCMethod::InternalTxSliceReadKeysResult: {

                        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();

                        PbRpcInternalTxSliceReadKeysResult opResult;
                        opResult.ParseFromString(rpcRequest.arguments());

                        int part = opResult.srcpartition();
                        int dc = opResult.srcdatacenter();

                        Transaction *tx = _coordinator->GetTransaction(opResult.id());

                        TxReadSlice *slice = tx->partToReadSliceMap[part];

                        slice->values.clear();
                        slice->values.resize(opResult.value_size(), "");

                        for (int i = 0; i < opResult.value_size(); i++) {
                            slice->values[i] = opResult.value(i);
                        }

                        slice->txWaitOnReadTime = opResult.waitedxact();
                        slice->sucesses = opResult.succeeded();
                        tx->readSlicesWaitHandle->SetIfCountZero();

                        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
                        double duration = (endTime - startTime).toMilliSeconds();
                        SysStats::CoordinatorReadReplyHandlingLatencySum =
                                SysStats::CoordinatorReadReplyHandlingLatencySum + duration;
                        SysStats::NumCoordinatorReadReplyHandlingMeasurements++;

                        break;
                    }

                    case RPCMethod::SendLST: {

                        PbRpcLST pb_st;
                        std::string arg;
                        pb_st.ParseFromString(arg = rpcRequest.arguments());

#ifdef MEASURE_STATISTICS
                        SysStats::NumRecvGSTs += 1;
                        SysStats::NumRecvGSTBytes += arg.size();
#endif

                        PhysicalTimeSpec lst;

                        lst.Seconds = pb_st.lst().seconds();
                        lst.NanoSeconds = pb_st.lst().nanoseconds();

                        int round = pb_st.round();

                        if (SysConfig::GSTDerivationMode == GSTDerivationType::TREE) {
                            _coordinator->HandleLSTFromChildren(lst, round);


                        } else {
                            std::cout << "Nonexistent GSTDerivationType: \n";
                        }
                        break;
                    }


                    case RPCMethod::SendGST: {

                        PbRpcGST pb_st;
                        std::string arg;

                        pb_st.ParseFromString(arg = rpcRequest.arguments());

#ifdef MEASURE_STATISTICS
                        SysStats::NumRecvGSTBytes += arg.size();
                        SysStats::NumRecvGSTs++;
#endif
                        PhysicalTimeSpec gst;

                        gst.Seconds = pb_st.gst().seconds();
                        gst.NanoSeconds = pb_st.gst().nanoseconds();

                        _coordinator->HandleGSTFromParent(gst);

                        break;
                    }

                    case RPCMethod::SendGSTAndUST: {

                        PbRpcGSTandUST pb_st;
                        std::string arg;
                        pb_st.ParseFromString(arg = rpcRequest.arguments());
#ifdef MEASURE_STATISTICS
                        SysStats::NumRecvGSTBytes += arg.size();
                        SysStats::NumRecvGSTs++;
#endif
                        PhysicalTimeSpec gst, ust;

                        gst.Seconds = pb_st.gst().seconds();
                        gst.NanoSeconds = pb_st.gst().nanoseconds();

                        ust.Seconds = pb_st.ust().seconds();
                        ust.NanoSeconds = pb_st.ust().nanoseconds();

                        _coordinator->HandleGSTAndUSTFromParent(gst, ust);

                        break;
                    }


                    default:
                        throw KVOperationException("(partition) Unsupported operation.");
                }
            }
        } catch (SocketException &e) {

            fprintf(stdout, "Partition serving thread socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "SEVERE Server partition socket exception: %s\n", "...");
            fflush(stdout);
        }


    }


    void KVTxServer::HandlePartitionsWriteRequest(TCPSocket *clientSocket) {
        try {
            RPCServerPtr rpcServer(new RPCServer(clientSocket));
            DBPartition servedPartition;

            for (;;) {
                // get rpc request
                PbRpcRequest rpcRequest;
                PbRpcReply rpcReply;
                rpcServer->RecvRequest(rpcRequest);

                switch (static_cast<RPCMethod> (rpcRequest.methodid())) {

                    case RPCMethod::InternalPrepareRequest: {
                        PbRpcPrepareRequestArg opArg;
                        opArg.ParseFromString(rpcRequest.arguments());

                        HandleInternalPrepareRequest<PbRpcPrepareRequestArg>(opArg);


                        break;
                    }

                    case RPCMethod::InternalPrepareReply: {

                        //You already got the result from the queried partition
                        PbRpcInternalPrepareReplyResult opResult;
                        opResult.ParseFromString(rpcRequest.arguments());

                        PhysicalTimeSpec pt;
                        pt.Seconds = opResult.pt().seconds();
                        pt.NanoSeconds = opResult.pt().nanoseconds();

                        _coordinator->HandleInternalPrepareReply(opResult.id(), opResult.srcpartition(), pt,
                                                                 opResult.blockduration());

                        break;
                    }

                    case RPCMethod::InternalCommitRequest: {

                        PbRpCommitRequestArg opArg;
                        PhysicalTimeSpec ct;

                        opArg.ParseFromString(rpcRequest.arguments());
                        unsigned int txId = opArg.id();
                        ct.Seconds = opArg.ct().seconds();
                        ct.NanoSeconds = opArg.ct().nanoseconds();

                        _coordinator->HandleInternalCommitRequest(txId, ct);

                        break;
                    }
                    default:
                        throw KVOperationException("(partition) Unsupported operation.");
                }
            }
        } catch (SocketException &e) {

            fprintf(stdout, "Partition serving thread socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "SEVERE Server partition socket exception: %s\n", "...");
            fflush(stdout);
        }
    }


    template<class Argument, class Result>
    void KVTxServer::HandleInternalTxSliceReadKeys(Argument &opArg, Result &opResult) {

        TxContex cdata;
        std::vector<std::string> readValues;
        std::vector<std::string> readKeys;
        bool ret;

        int srcPartition = opArg.srcpartition();
        int srcDC = opArg.srcdatacenter();

#ifndef EVENTUAL_CONSISTENCY
        const PbPhysicalTimeSpec &w = opArg.ust();
        cdata.UST.Seconds = w.seconds();
        cdata.UST.NanoSeconds = w.nanoseconds();
#endif //EVENTUAL_CONSISTENCY

        readValues.resize(opArg.key_size(), "");
        for (int i = 0; i < opArg.key_size(); ++i) {
            readKeys.push_back(opArg.key(i));
        }


        ret = _coordinator->InternalTxSliceReadKeys(opArg.id(), cdata, readKeys, readValues);

        opResult.set_succeeded(ret);

        if (ret) {
            for (int i = 0; i < opArg.key_size(); i++) {
                opResult.add_value(readValues[i]);
            }

#ifdef BLOCKING_ON
#ifdef MEASURE_STATISTICS
            assert(cdata.waited_xact >= 0);
            opResult.set_waitedxact(cdata.waited_xact);
#endif
#endif


        } else {
            fprintf(stdout, "Error in HandleInternalTxSliceReadKeys .\n");
            fflush(stdout);
        }
    }

    template<class Argument>
    void KVTxServer::HandleInternalPrepareRequest(Argument &opArg) {
        TxContex cdata;
        std::vector<std::string> keys;
        std::vector<std::string> vals;

        int txId = opArg.id();

#ifndef EVENTUAL_CONSISTENCY
        const PbPhysicalTimeSpec &w = opArg.ust();
        cdata.UST.Seconds = w.seconds();
        cdata.UST.NanoSeconds = w.nanoseconds();
#endif //EVENTUAL_CONSISTENCY

        const PbPhysicalTimeSpec &y = opArg.ht();
        cdata.HT.Seconds = y.seconds();
        cdata.HT.NanoSeconds = y.nanoseconds();

        for (int i = 0; i < opArg.key_size(); ++i) {
            keys.push_back(opArg.key(i));
            vals.push_back(opArg.value(i));
        }

        int srcPartition = opArg.srcpartition();
        int srcDataCenter = opArg.srcdatacenter();

        _coordinator->InternalPrepareRequest(txId, cdata, keys, vals, srcPartition, srcDataCenter);
    }

    void KVTxServer::ServeReplicationConnection() {
        try {
            TCPServerSocket serverSocket(_replicationPort);
            for (;;) {
                TCPSocket *clientSocket = serverSocket.accept();
                std::thread t(&KVTxServer::HandleReplicationRequest, this, clientSocket);
                t.detach();
            }
        } catch (SocketException &e) {

            fprintf(stdout, "Server replication socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "SEVERE Server partition socket exception: %s\n", "...");
            fflush(stdout);
        }
    }


    void KVTxServer::HandleReplicationRequest(TCPSocket *clientSocket) {
        try {
            RPCServerPtr rpcServer(new RPCServer(clientSocket));
            DBPartition servedPartition;

            for (;;) {
                // get rpc request
                PbRpcRequest rpcRequest;
                rpcServer->RecvRequest(rpcRequest);
                switch (static_cast<RPCMethod> (rpcRequest.methodid())) {
                    case RPCMethod::InitializeReplication: {

                        PbPartition opArg;
                        opArg.ParseFromString(rpcRequest.arguments());

                        servedPartition.Name = opArg.name();
                        servedPartition.PublicPort = opArg.publicport();
                        servedPartition.PartitionPort = opArg.partitionport();
                        servedPartition.ReplicationPort = opArg.replicationport();
                        servedPartition.PartitionId = opArg.partitionid();
                        servedPartition.ReplicaId = opArg.replicaid();

                        break;
                    }

                    case RPCMethod::ReplicateUpdate: {
                        // get arguments
                        PbRpcReplicationArg opArg;
                        std::string arg;
                        opArg.ParseFromString(arg = rpcRequest.arguments());

#ifdef MEASURE_STATISTICS
                        SysStats::NumRecvReplicationBytes += arg.size();
#endif
                        // apply propagated updates
                        std::vector<PropagatedUpdate *> updates;

                        for (int i = 0; i < opArg.updaterecord_size(); i++) {
                            PropagatedUpdate *update = new PropagatedUpdate();

                            PbLogRecord record;

                            const std::string &serializedRecord = opArg.updaterecord(i);
                            record.ParseFromString(serializedRecord);
                            update->SerializedRecord = serializedRecord;

                            update->SrcReplica = servedPartition.ReplicaId;
                            update->Key = record.key();
                            update->Value = record.value();
                            update->UT.Seconds = record.ut().seconds();
                            update->UT.NanoSeconds = record.ut().nanoseconds();
                            update->LUT = record.lut();

#ifdef MEASURE_VISIBILITY_LATENCY
                            update->CreationTime.Seconds = record.creationtime().seconds();
                            update->CreationTime.NanoSeconds = record.creationtime().nanoseconds();
#endif


                            updates.push_back(update);
                        }

                        _coordinator->HandlePropagatedUpdate(updates);

                        break;
                    }
                    case RPCMethod::SendHeartbeat: {

                        // get arguments
                        PbRpcHeartbeat pb_hb;

                        pb_hb.ParseFromString(rpcRequest.arguments());

                        Heartbeat hb;

                        hb.PhysicalTime.Seconds = pb_hb.physicaltime().seconds();
                        hb.PhysicalTime.NanoSeconds = pb_hb.physicaltime().nanoseconds();

                        hb.LogicalTime = pb_hb.logicaltime();

                        _coordinator->HandleHeartbeat(hb, servedPartition.ReplicaId);

                        break;
                    }

                    case RPCMethod::PushGST: {
                        PbRpcGlobalStabilizationTime argGST;
                        std::string arg;
                        argGST.ParseFromString(arg = rpcRequest.arguments());
#ifdef MEASURE_STATISTICS
                        SysStats::NumRecvUSTBytes += arg.size();
                        SysStats::NumRecvUSTs += 1;
#endif
                        PhysicalTimeSpec recvGST;
                        recvGST.Seconds = argGST.time().seconds();
                        recvGST.NanoSeconds = argGST.time().nanoseconds();

                        int srcDC = argGST.srcdc();

                        _coordinator->HandleGSTPush(recvGST, srcDC);
                        break;
                    }

                    default:
                        throw KVOperationException("(replication) Unsupported operation\n");

                }
            }
        } catch (SocketException &e) {
            fprintf(stdout, "HandleReplicationRequest:Client serving thread socket exception: %s\n", e.what());
            fflush(stdout);
        }
        catch (...) {
            fprintf(stdout, "SEVERE Server partition socket exception: %s\n", "...");
            fflush(stdout);
        }
    }


} // namespace scc
