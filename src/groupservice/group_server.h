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



#ifndef SCC_GROUPSERVICE_TIME_SERVER_H
#define SCC_GROUPSERVICE_TIME_SERVER_H

#include "common/types.h"
#include "rpc/socket.h"
#include "messages/rpc_messages.pb.h"
#include <string>
#include <vector>
#include <map>
#include <thread>
#include <queue>
#include <ostream>

typedef std::priority_queue<scc::ReplicaNode, std::vector<scc::ReplicaNode>, scc::CompareReplicaNodes> ReplicaUsageQueue;

namespace scc {

    class GroupServer {
    public:
        GroupServer(unsigned short port, int numPartitions, int numDCs, int replicationFactor);

        void Run();

        void HandleRequest(TCPSocket *clientSocket);

        void ServeConnection();

        void fillInMissingPartitions();

        void printRegisteredPartitions();

        void printPartitionMaps();


    private:
        unsigned short _serverPort;
        int _numPartitions;
        int _numDataCenters;
        int _partitionReplicationFactor; //TODO: Consider removing this (maybe it is not necessary
        int _totalNumPartitions;
        std::vector<std::vector<DBPartition>> _registeredPartitions;
        std::vector<std::vector<bool>> _registeredPartitionsPresenceMap;
        std::unordered_map<int, ReplicaUsageQueue *> pidToReplicasQ;
        int _numRegisteredPartitions;
        bool _allPartitionsRegistered;
        std::mutex _registrationMutex;
        int _numReadyPartitions;
        bool _allPartitionsReady;
        std::mutex _readinessMutex;

        struct ConvenienceMetricComparator {
            bool operator()(const ConvenienceMetrics *lhs, const ConvenienceMetrics *rhs) const {

                return lhs < rhs;
            }
        };

        std::unordered_map<int, std::map<int, ConvenienceMetrics, ConvenienceMetricComparator>> _partitionReplicasUsageMap; //key: partitionId, value map of  dcId to replicas convenience info
        int getMostConvenientRemoteReplicaIndex(int partitionId, int dcId) const;

        int getLeastUsedRemoteReplicaIndex(int partitionId, int dcId);

    };

}

#endif
