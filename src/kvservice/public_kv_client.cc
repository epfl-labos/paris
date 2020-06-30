#include "kvservice/public_kv_client.h"
#include "messages/rpc_messages.pb.h"
#include "messages/tx_messages.pb.h"
#include "common/sys_logger.h"
#include "common/utils.h"
#include "common/sys_stats.h"
#include "common/sys_config.h"
#include "common/exceptions.h"
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <time.h>
#include <iostream>

#ifndef USE_GSV_AS_DV
#error Tx only with USE_GSV_AS_DV
#endif

namespace scc {

    PublicTxClient::PublicTxClient(std::string serverName, int publicPort)
            : _serverName(serverName),
              _serverPort(publicPort) {
        _rpcClient = new SyncRPCClient(_serverName, _serverPort);
        GetServerConfig();
        _sessionData.txId = 1;
        _sessionData.WriteSet.clear();
        _sessionData.ReadSet.clear();
        _sessionData.WriteCache.clear();
        _sessionData.UST.setToZero();
        _sessionData.CT.setToZero();

#ifdef NO_START_TX
        _sessionData.txIdAssigned = false;
#endif //NO_START_TX

        numItemsReadFromStore = 0;
        numItemsReadFromClientWriteCache = 0;
        numItemsReadFromClientReadSet = 0;
        numItemsReadFromClientWriteSet = 0;
        totalNumReadItems = 0;
    }

    PublicTxClient::~PublicTxClient() {
        delete _rpcClient;
    }

    bool PublicTxClient::GetServerConfig() {
        std::string serializedArg;
        PbRpcTxPublicGetServerConfigResult result;
        std::string serializedResult;

        // call server
        _rpcClient->Call(RPCMethod::GetServerConfig, serializedArg, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (!result.succeeded()) {
            SLOG("Could not get server config!");
            return false;
        }

        _numPartitions = result.numpartitions();
        _numDataCenters = result.numdatacenters();
        _partitionReplicationFactor = result.partitionreplicationfactor();
        _replicaId = result.replicaid();

        return true;
    }

    int PublicTxClient::getLocalReplicaId() {
        return _replicaId;
    }

    int PublicTxClient::getNumDataCenters() {
        return _numDataCenters;
    }

    void PublicTxClient::Echo(const std::string &input, std::string &output) {
        PbRpcEchoTest arg;
        std::string serializedArg;
        PbRpcEchoTest result;
        std::string serializedResult;

        // prepare argument
        arg.set_text(input);
        serializedArg = arg.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::EchoTest, serializedArg, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        output = result.text();
    }

    void PublicTxClient::setTotalNumKeyInKVStore(int count) {
        totalNumKeyInKVStore = count;
    }

    void PublicTxClient::resetSession() {
        _sessionData.UST.setToZero();
        _sessionData.CT.setToZero();

        _sessionData.ReadSet.clear();
        _sessionData.WriteSet.clear();
        _sessionData.txId = 1;
    }


    bool PublicTxClient::TxStart() {

        std::string serializedArgs;
        std::string serializedResult;

        // prepare arguments
        PbRpcPublicStartArg args;
        PbRpcPublicStartResult result;

#ifndef EVENTUAL_CONSISTENCY

        args.mutable_ust()->set_seconds(_sessionData.UST.Seconds);
        args.mutable_ust()->set_nanoseconds(_sessionData.UST.NanoSeconds);

#endif //EVENTUAL_CONSISTENCY

        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::TxStart, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (result.succeeded()) {

            _sessionData.txId = result.id();

#ifndef EVENTUAL_CONSISTENCY

            PhysicalTimeSpec stResult(result.ust().seconds(), result.ust().nanoseconds());

            assert(stResult >= _sessionData.UST);
            _sessionData.UST.Seconds = result.ust().seconds();
            _sessionData.UST.NanoSeconds = result.ust().nanoseconds();

            trimWriteCache(_sessionData.UST);

#endif //EVENTUAL_CONSISTENCY

            _sessionData.ReadSet.clear();
            _sessionData.WriteSet.clear();
        }

        return result.succeeded();
    }

    bool PublicTxClient::TxRead(const std::vector<std::string> &keySet, std::vector<std::string> &valueSet) {

#ifdef NO_START_TX

        if (!_sessionData.txIdAssigned) {
            _sessionData.ReadSet.clear();
            _sessionData.WriteSet.clear();
        }


#endif //NO_START_TX

        std::string serializedArgs;
        std::string serializedResult;
        std::string PLACEHOLDER = "";
        unsigned int i;

        // prepare arguments

        PbRpcPublicReadArg args;
        PbRpcPublicReadResult result;

        std::vector<std::string> unseenKeys;
        std::vector<int> positions;

        totalNumReadItems += keySet.size();

        for (i = 0; i < keySet.size(); ++i) {
            valueSet.push_back(PLACEHOLDER);

            /* If the key has been written/updated in this same transaction, read that value. */
            if (_sessionData.WriteSet.find(keySet[i]) != _sessionData.WriteSet.end()) {
                valueSet[i] = _sessionData.WriteSet[keySet[i]];
                numItemsReadFromClientWriteSet++;

            } else if (_sessionData.ReadSet.find(keySet[i]) != _sessionData.ReadSet.end()) {
                /* If the key has been read before in this same transaction, read that value (enabling repeatable reads). */
                valueSet[i] = _sessionData.ReadSet[keySet[i]];
                numItemsReadFromClientReadSet++;

            } else {

                /* The key has been writen from the client in a previous transaction */
                /* If the key is still present in the write cache, its value is newer */
                /* the snapshot time, and that is the value that needs to be returned */

#ifndef EVENTUAL_CONSISTENCY

                auto it = _sessionData.WriteCache.find(keySet[i]);
                bool readFromCache;

                readFromCache = it != _sessionData.WriteCache.end();

                if (readFromCache) {
                    valueSet[i] = it->second.first;
                    numItemsReadFromClientWriteCache++;
                } else {
                    unseenKeys.push_back(keySet[i]);
                    positions.push_back(i);
                }

#else //eventually consistent version

                unseenKeys.push_back(keySet[i]);
                positions.push_back(i);

#endif //EVENTUAL_CONSISTENCY

            }
        }

        // prepare tx id argument
        args.set_id(_sessionData.txId);

        for (i = 0; i < unseenKeys.size(); ++i) {
            args.add_key(unseenKeys[i]);
        }

#ifdef NO_START_TX
        if (!_sessionData.txIdAssigned) {
            args.set_notxid(true);
        }
#endif //NO_START_TX

        numItemsReadFromStore += unseenKeys.size();

        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::TxRead, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (result.succeeded()) {

#ifdef NO_START_TX
            _sessionData.txId = result.id();
#endif //NO_START_TX

            ASSERT(unseenKeys.size() == result.value_size());
            for (int i = 0; i < result.value_size(); ++i) {
                _sessionData.ReadSet[keySet[positions[i]]] = result.value(i);
                assert(_sessionData.ReadSet.find(keySet[positions[i]]) != _sessionData.ReadSet.end());
                valueSet[positions[i]] = result.value(i);

            }
        } else {
            SLOG("[ERROR]:UNSUCCESSFUL READ!");
        }

#ifdef NO_START_TX
        _sessionData.txIdAssigned = true;
#endif

        return result.succeeded();
    }


    bool PublicTxClient::TxWrite(const std::vector<std::string> &keySet, std::vector<std::string> &valueSet) {
        assert(keySet.size() == valueSet.size());

        for (unsigned int i = 0; i < keySet.size(); ++i) {
            /* Update an item that has already been updated within the same transaction. */
            _sessionData.WriteSet[keySet[i]] = valueSet[i];
            assert(_sessionData.WriteSet.find(keySet[i]) != _sessionData.WriteSet.end());
        }

        return true;
    }


    bool PublicTxClient::TxCommit() {

        PbRpcPublicCommitArg args;
        PbRpcPublicCommitResult result;

        std::string serializedArgs;
        std::string serializedResult;

        // prepare arguments
        args.set_id(_sessionData.txId);

        if (!_sessionData.WriteSet.empty()) {
            for (auto it = _sessionData.WriteSet.begin(); it != _sessionData.WriteSet.end(); ++it) {
                args.add_key(it->first);
                args.add_value(it->second);
            }

            args.mutable_lct()->set_seconds(_sessionData.CT.Seconds);
            args.mutable_lct()->set_nanoseconds(_sessionData.CT.NanoSeconds);
        }

        serializedArgs = args.SerializeAsString();

        _rpcClient->Call(RPCMethod::TxCommit, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (!_sessionData.WriteSet.empty() && result.succeeded()) {

            PhysicalTimeSpec ctResult;
            ctResult.Seconds = result.ct().seconds();
            ctResult.NanoSeconds = result.ct().nanoseconds();
            assert(ctResult > _sessionData.CT);
            _sessionData.CT.Seconds = result.ct().seconds();
            _sessionData.CT.NanoSeconds = result.ct().nanoseconds();

#ifndef EVENTUAL_CONSISTENCY
            updateWriteCache(_sessionData.CT);
#endif //EVENTUAL_CONSISTENCY

        }

#ifdef NO_START_TX
        _sessionData.txIdAssigned = false;
#endif //NO_START_TX

        return true;
    }


    bool PublicTxClient::ShowItem(const std::string &key, std::string &itemVersions) {
        PbRpcKVPublicShowArg args;
        std::string serializedArgs;
        PbRpcKVPublicShowResult result;
        std::string serializedResult;

        // prepare arguments
        args.set_key(key);
        serializedArgs = args.SerializeAsString();

        // call server
        _rpcClient->Call(RPCMethod::ShowItem, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            itemVersions = result.returnstring();
        }

        return result.succeeded();
    }

    bool PublicTxClient::ShowDB(std::string &allItemVersions) {
        std::string serializedArgs;
        PbRpcKVPublicShowResult result;
        std::string serializedResult;

        // call server
        _rpcClient->Call(RPCMethod::ShowDB, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            allItemVersions = result.returnstring();
        }

        return result.succeeded();
    }

    bool PublicTxClient::ShowState(std::string &stateStr) {
        std::string serializedArgs;
        PbRpcKVPublicShowResult result;
        std::string serializedResult;

        // call server
        _rpcClient->Call(RPCMethod::ShowState, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            stateStr = result.returnstring();
        }

        return result.succeeded();
    }

    bool PublicTxClient::ShowStateCSV(std::string &stateStr) {
        std::string serializedArgs;
        PbRpcKVPublicShowResult result;
        std::string serializedResult;

        // call server
        _rpcClient->Call(RPCMethod::ShowStateCSV, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            stateStr = result.returnstring();
        }

        return result.succeeded();
    }

    bool PublicTxClient::DumpLatencyMeasurement(std::string &resultStr) {
        std::string serializedArgs;
        PbRpcKVPublicShowResult result;
        std::string serializedResult;

        // call server
        _rpcClient->Call(RPCMethod::DumpLatencyMeasurement, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);
        if (result.succeeded()) {
            resultStr = result.returnstring();
        }

        return result.succeeded();
    }


    void PublicTxClient::trimWriteCache(PhysicalTimeSpec t) {

        for (auto it = begin(_sessionData.WriteCache); it != end(_sessionData.WriteCache);) {
            PhysicalTimeSpec updateTime = it->second.second;

            if (updateTime <= t) {
                it = _sessionData.WriteCache.erase(it);
            } else {
                ++it;
            }
        }
    }

    void PublicTxClient::updateWriteCache(PhysicalTimeSpec ct) {
        for (auto it = begin(_sessionData.WriteSet); it != end(_sessionData.WriteSet); ++it) {
            std::string writeSetKey = it->first;
            std::string writeSetValue = it->second;

            _sessionData.WriteCache[writeSetKey] = std::make_pair(writeSetValue, ct);
        }
    }


    int PublicTxClient::getNumItemsReadFromClientWriteSet() {
        return numItemsReadFromClientWriteSet;
    }

    int PublicTxClient::getNumItemsReadFromClientReadSet() {
        return numItemsReadFromClientReadSet;
    }

    int PublicTxClient::getNumItemsReadFromClientWriteCache() {
        return numItemsReadFromClientWriteCache;
    }

    int PublicTxClient::getNumItemsReadFromStore() {
        return numItemsReadFromStore;
    }

    int PublicTxClient::getTotalNumReadItems() {
        return totalNumReadItems;
    }

    bool PublicTxClient::getRemotePartitionsIds(std::vector<int> &ids) {
        std::string serializedArgs;
        PbRpcPublicGetRemotePartitionsIdsResult result;
        std::string serializedResult;


        // call server
        _rpcClient->Call(RPCMethod::GetRemotePartitionsIds, serializedArgs, serializedResult);

        // parse result
        result.ParseFromString(serializedResult);

        if (result.succeeded()) {
            for (int i = 0; i < result.pids_size(); i++) {
                ids.push_back(result.pids(i));
            }

        } else {
            SLOG("[ERROR]:UNSUCCESSFUL GET OF REMOTE PARTITIONS IDS!");
        }

        return result.succeeded();
    }


} // namespace scc