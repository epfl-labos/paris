#include "common/utils.h"
#include "kvservice/partition_kv_client.h"
#include "common/sys_config.h"
#include "messages/rpc_messages.pb.h"
#include "messages/tx_messages.pb.h"
#include "common/sys_logger.h"
#include "common/sys_stats.h"

namespace scc {

    PartitionKVClient::PartitionKVClient(std::string serverName, int serverPort)
            : _serverName(serverName),
              _serverPort(serverPort),
              _asyncRpcClient(NULL),
              _syncRpcClient(NULL) {
        _asyncRpcClient = new AsyncRPCClient(serverName, serverPort, SysConfig::NumChannelsPartition);
        _syncRpcClient = new SyncRPCClient(serverName, serverPort);
    }

    PartitionKVClient::~PartitionKVClient() {
        delete _asyncRpcClient;
    }

    bool PartitionKVClient::ShowItem(const std::string &key, std::string &itemVersions) {
        PbRpcKVInternalShowItemArg args;
        std::string serializedArgs;
        PbRpcKVInternalShowItemResult result;
        AsyncState state;

        // prepare arguments
        args.set_key(key);
        serializedArgs = args.SerializeAsString();

        // call server
        _asyncRpcClient->Call(RPCMethod::InternalShowItem, serializedArgs, state);
        state.FinishEvent.WaitAndReset();

        // parse result
        result.ParseFromString(state.Results);
        if (result.succeeded()) {
            itemVersions = result.itemversions();
        }

        return result.succeeded();
    }

    void PartitionKVClient::InitializePartitioning(DBPartition source) {
        PbPartition opArg;
        opArg.set_name(source.Name);
        opArg.set_publicport(source.PublicPort);
        opArg.set_partitionport(source.PartitionPort);
        opArg.set_replicationport(source.ReplicationPort);
        opArg.set_partitionid(source.PartitionId);
        opArg.set_replicaid(source.ReplicaId);

        std::string serializedArg = opArg.SerializeAsString();

        // call server
        _syncRpcClient->Call(RPCMethod::InitializePartitioning, serializedArg);
    }


    void PartitionKVClient::PrepareRequest(int txId, TxContex &cdata, const std::vector<std::string> &keys,
                                           std::vector<std::string> &values, int srcPartitionId, int srcDataCenterId) {

        PbRpcPrepareRequestArg opArg;

        // Set the key to be read and the src node
        opArg.set_id(txId);

#ifndef EVENTUAL_CONSISTENCY
        opArg.mutable_ust()->set_seconds(cdata.UST.Seconds);
        opArg.mutable_ust()->set_nanoseconds(cdata.UST.NanoSeconds);
#endif //EVENTUAL_CONSISTENCY

        opArg.mutable_ht()->set_seconds(cdata.HT.Seconds);
        opArg.mutable_ht()->set_nanoseconds(cdata.HT.NanoSeconds);

        for (int i = 0; i < keys.size(); i++) {
            opArg.add_key(keys[i]);
            opArg.add_value(values[i]);
        }

        opArg.set_srcpartition(srcPartitionId);
        opArg.set_srcdatacenter(srcDataCenterId) ;

        std::string serializedArg = opArg.SerializeAsString();

        _asyncRpcClient->CallWithNoState(RPCMethod::InternalPrepareRequest, serializedArg);


    }


    void PartitionKVClient::CommitRequest(unsigned int txId, PhysicalTimeSpec ct) {


        PbRpCommitRequestArg opArg;

        opArg.set_id(txId);

        opArg.mutable_ct()->set_seconds(ct.Seconds);
        opArg.mutable_ct()->set_nanoseconds(ct.NanoSeconds);

        std::string serializedArg = opArg.SerializeAsString();

        assert(_asyncRpcClient != NULL);
        _asyncRpcClient->CallWithNoState(RPCMethod::InternalCommitRequest, serializedArg);

    }





    void PartitionKVClient::TxSliceReadKeys(unsigned int txId, TxContex &cdata, const std::vector<std::string> &keys,
                                            int srcPartitionId, int srcDataCenterId) {

        PbRpcInternalTxSliceReadKeysArg opArg;
        // Set the key to be read and the src node
        for (int i = 0; i < keys.size(); i++) {
            opArg.add_key(keys[i]);
        }

        opArg.set_srcpartition(srcPartitionId);
        opArg.set_srcdatacenter(srcDataCenterId);
        opArg.set_id(txId);

#ifndef EVENTUAL_CONSISTENCY
        opArg.mutable_ust()->set_seconds(cdata.UST.Seconds);
        opArg.mutable_ust()->set_nanoseconds(cdata.UST.NanoSeconds);
#endif //EVENTUAL_CONSISTENCY

        std::string serializedArg = opArg.SerializeAsString();

        _asyncRpcClient->CallWithNoState(RPCMethod::InternalTxSliceReadKeys, serializedArg);

    }

    void PartitionKVClient::SendLST(PhysicalTimeSpec lst, int round) {
        PbRpcLST pb_lst;
        pb_lst.mutable_lst()->set_seconds(lst.Seconds);
        pb_lst.mutable_lst()->set_nanoseconds(lst.NanoSeconds);
        pb_lst.set_round(round);

        std::string serializedArg = pb_lst.SerializeAsString();

#ifdef MEASURE_STATISTICS
        SysStats::NumSentGSTBytes += serializedArg.size();
        SysStats::NumSentGSTs += 1;
#endif
        // call server
        _syncRpcClient->Call(RPCMethod::SendLST, serializedArg);
    }


    void PartitionKVClient::SendGST(PhysicalTimeSpec gst) {
//        PbRpcGST pb_st;
//
//        pb_st.mutable_gst()->set_seconds(gst.Seconds);
//        pb_st.mutable_gst()->set_nanoseconds(gst.NanoSeconds);
//
//        std::string serializedArg = pb_st.SerializeAsString();
//
//#ifdef MEASURE_STATISTICS
//        SysStats::NumSentGSTBytes += serializedArg.size();
//        SysStats::NumSentGSTs += 1;
//#endif
//
//        // call server
//        _syncRpcClient->Call(RPCMethod::SendGST, serializedArg);
    }


    void PartitionKVClient::SendGSTAndUST(PhysicalTimeSpec global, PhysicalTimeSpec universal) {
        PbRpcGSTandUST pb_st;

        pb_st.mutable_gst()->set_seconds(global.Seconds);
        pb_st.mutable_gst()->set_nanoseconds(global.NanoSeconds);

        pb_st.mutable_ust()->set_seconds(universal.Seconds);
        pb_st.mutable_ust()->set_nanoseconds(universal.NanoSeconds);

        std::string serializedArg = pb_st.SerializeAsString();

#ifdef MEASURE_STATISTICS
        SysStats::NumSentGSTBytes += serializedArg.size();
        SysStats::NumSentGSTs += 1;
#endif

        // call server
        _syncRpcClient->Call(RPCMethod::SendGSTAndUST, serializedArg);
    }


}
