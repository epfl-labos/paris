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



#ifndef SCC_KVSERVICE_PARTITION_KV_TX_CLIENT_H
#define SCC_KVSERVICE_PARTITION_KV_TX_CLIENT_H

#include "common/types.h"
#include "rpc/async_rpc_client.h"
#include "rpc/sync_rpc_client.h"
#include <string>
#include <vector>

namespace scc {

    class PartitionKVClient {
    public:
        PartitionKVClient(std::string serverName, int serverPort);

        ~PartitionKVClient();

        bool ShowItem(const std::string &key, std::string &itemVersions);

        void InitializePartitioning(DBPartition source); // done by a different connection

        void SendLST(PhysicalTimeSpec lst, int round);

        void SendGST(PhysicalTimeSpec gst);

        void SendGSTAndUST(PhysicalTimeSpec global, PhysicalTimeSpec universal);


        void
        TxSliceReadKeys(unsigned int txId, TxContex &cdata, const std::vector<std::string> &keys, int srcPartitionId,
                        int srcDataCenterId);

        void PrepareRequest(int txId, TxContex &cdata, const std::vector<std::string> &keys,
                            std::vector<std::string> &values, int srcPartitionId, int srcDataCenterId);


        void CommitRequest(unsigned int txId, PhysicalTimeSpec ct);

        template<class Result>
        void SendInternalTxSliceReadKeysResult(Result &opResult) {
            std::string serializedArg = opResult.SerializeAsString();
            _asyncRpcClient->CallWithNoState(RPCMethod::InternalTxSliceReadKeysResult, serializedArg);
        }

        template<class Result>
        void SendPrepareReply(Result &opResult) {
            std::string serializedArg = opResult.SerializeAsString();
            _asyncRpcClient->CallWithNoState(RPCMethod::InternalPrepareReply, serializedArg);
        }

        /* Stabilization protocol */

        void SendStabilizationTimesToPeers(PhysicalTimeSpec lst, PhysicalTimeSpec rst, int partitionId);

    private:
        std::string _serverName;
        int _serverPort;
        AsyncRPCClient *_asyncRpcClient;
        SyncRPCClient *_syncRpcClient;

    };
}

#endif
