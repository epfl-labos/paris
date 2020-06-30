/*
 * PaRiS 
 *
 * Copyright 2019 Operating Systems Laboratory EPFL
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



#ifndef SCC_KVSERVICE_PUBLIC_TX_CLIENT_H
#define SCC_KVSERVICE_PUBLIC_TX_CLIENT_H

#include "common/types.h"
#include "rpc/sync_rpc_client.h"
#include "rpc/rpc_id.h"
#include <string>
#include <vector>
#include <unordered_set>

namespace scc {

    class PublicTxClient {
    public:
        PublicTxClient(std::string serverName, int serverPort);

        PublicTxClient(std::string serverName, int serverPort, int numCtxs);

        ~PublicTxClient();

        void Echo(const std::string &input, std::string &output);

        bool TxStart();

        bool TxRead(const std::vector<std::string> &keySet, std::vector<std::string> &valueSet);

        bool TxWrite(const std::vector<std::string> &keySet, std::vector<std::string> &valueSet);

        bool TxCommit();

        bool ShowItem(const std::string &key, std::string &itemVersions);

        bool ShowDB(std::string &allItemVersions); // TBD
        bool ShowState(std::string &stateStr);

        bool ShowStateCSV(std::string &stateStr);

        bool DumpLatencyMeasurement(std::string &resultStr); // TBD

        void resetSession();

        void setTotalNumKeyInKVStore(int count);

        int getLocalReplicaId();

        int getNumDataCenters();

        int getNumItemsReadFromClientWriteSet();

        int getNumItemsReadFromClientReadSet();

        int getNumItemsReadFromClientWriteCache();

        int getNumItemsReadFromStore();

        int getTotalNumReadItems();

        bool getRemotePartitionsIds(std::vector<int> &ids);

    private:
        std::string _serverName;
        int _serverPort;
        SyncRPCClient *_rpcClient;
        int _numPartitions;
        int _numDataCenters;
        int _partitionReplicationFactor;
        int _replicaId;
        int totalNumKeyInKVStore;
        TxClientMetadata _sessionData;
        int totalNumReadItems;
        int numItemsReadFromClientWriteSet;
        int numItemsReadFromClientReadSet;
        int numItemsReadFromClientWriteCache;
        int numItemsReadFromStore;

    private:
        bool GetServerConfig();


        void trimWriteCache(PhysicalTimeSpec t);

        void updateWriteCache(PhysicalTimeSpec ct);
    };

} // namespace scc

#endif
