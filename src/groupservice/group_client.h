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



#ifndef SCC_TIMESERVICE_TIME_CLIENT_H
#define SCC_TIMESERVICE_TIME_CLIENT_H

#include "common/types.h"
#include "rpc/sync_rpc_client.h"
#include <string>
#include <vector>

namespace scc {

    class GroupClient {
    public:
        GroupClient();

        GroupClient(std::string host, int port);

        // common interfaces used by all protocols ̰ 
        void Echo(const std::string &text, std::string &echoText);

        bool RegisterPartition(DBPartition &p);

        std::vector<std::vector<DBPartition>> GetRegisteredPartitions();

        std::vector<std::pair<int, int>> GetPartitionUsersIds(int pId, int rId);
        //ids of the remote partitions that use this partition as substitution for a missing one in their DC

        bool NotifyReadiness(DBPartition &p);

        int GetReplicationFactor();

    private:
        SyncRPCClient _rpcClient;
    };

}

#endif
