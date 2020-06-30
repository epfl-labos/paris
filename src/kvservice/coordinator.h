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



#ifndef SCC_KVSERVICE_COORDINATOR_H
#define SCC_KVSERVICE_COORDINATOR_H

#include "common/types.h"
#include "common/sys_config.h"
#include "groupservice/group_client.h"
#include "kvservice/transaction.h"
#include "kvservice/replication_kv_client.h"
#include "kvservice/partition_kv_client.h"
#include <string>
#include <vector>
#include <map>
#include <set>
#include <boost/thread.hpp>
#include <math.h>


#ifdef XACT_SPIN
#define LOCK_XACT()  do{\
                        while(!__sync_bool_compare_and_swap((volatile unsigned int *)&_tx_lock, 0, 1)){\
                                __asm __volatile("pause\n": : :"memory");\
                            }\
                    }while(0)

#define UNLOCK_XACT() *(volatile unsigned int *)&_tx_lock = 0;

#else

#define LOCK_XACT() {std::lock_guard <std::mutex> lk(_tx_mutex);

#define UNLOCK_XACT() }
#endif


#define LOCK_HLC() {std::lock_guard <std::mutex> lk(_hlc_mutex);

#define UNLOCK_HLC() }


#define LOCK_PREPARED_REQUESTS() {std::lock_guard <std::mutex> lk(preparedRequestsMutex);

#define UNLOCK_PREPARED_REQUESTS() }

#define LOCK_COMMIT_REQUESTS() {std::lock_guard <std::mutex> lk(commitRequestsMutex);

#define UNLOCK_COMMIT_REQUESTS() }

#define LOCAL_GST MVKVTXStore::Instance()->_GSTs[_replicaId]

namespace scc {

    enum class TreeNodeType;

    class TreeNode;

    class CoordinatorTx {
    public:
        CoordinatorTx(std::string serverName, int publicPort, int totalNumKeys);

        CoordinatorTx(std::string serverName,
                      int publicPort,
                      int partitionPort,
                      int replicationPort,
                      int partitionId,
                      int replicaId,
                      int totalNumKeys,
                      std::string groupServerName,
                      int groupServerPort,
                      int replicationFactor);

        ~CoordinatorTx();

        // initialization
        void Initialize();

        // key-value interfaces
        void Echo(const std::string &input, std::string &output);

        bool Add(const std::string &key, const std::string &value);

        // debugging
        bool ShowItem(const std::string &key, std::string &itemVersions);

        bool ShowDB(std::string &allItemVersions);

        bool ShowState(std::string &stateStr);

        bool ShowStateCSV(std::string &stateStr);

        bool DumpLatencyMeasurement(std::string &resultStr);

        // auxiliary functions
        int NumPartitions() {
            return _numPartitions;
        }

        int NumDataCenters() {
            return _numDataCenters;
        }

        int PartitionReplicationFactor() {
            return _partitionReplicationFactor;
        }

        int ReplicaId() {
            return _replicaId;
        }

        int PartitionId() {
            return _partitionId;
        }

        Transaction *GetTransaction(unsigned long id);

        inline unsigned long getAndIncrementTxCount() {
            unsigned long old = _tx_counter;
            _tx_counter += (_numPartitions * _partitionReplicationFactor);
            return old;
        }

        /* initialization functions */

        void initializeReplicasAndPartitions();

        void loadKeys() const;

        void connectToRemoteReplicas();

        void connectToStabilizationPeers();

        void pushGlobalStabilizationTimeToDCPeers(PhysicalTimeSpec gst);

        void HandleGSTPush(PhysicalTimeSpec gst, int srcDC);

        void HandleGSTAndUSTFromParent(PhysicalTimeSpec parent_gst, PhysicalTimeSpec ust);


        void connectToLocalPartitions();

        void connectToRemotePartitions();

        void connectToRemoteAdoptivePartitions();

        std::vector<int> getRemotePartitionsIds();


        /* ************************   Start of transaction related functions    ************************ */
        bool TxStart(TxConsistencyMetadata &cdata);

        /* ************************    Transactional read related functions     ************************ */

        bool TxRead(int txId, const std::vector<std::string> &keySet, std::vector<std::string> &valueSet);


        void mapKeysToPartitionInReadPhase(const vector<string, allocator<string>> &keySet,
                                           vector<string, allocator<string>> &valueSet,
                                           std::unordered_map<int, TxReadSlice *> &txPartitionToReadSliceMap);

        void readLocalKeys(Transaction *tx);

        void sendTxReadSliceRequestsToOtherPartitions(Transaction *tx);


        bool InternalTxSliceReadKeys(unsigned int txId, TxContex &cdata, const std::vector<std::string> &keys,
                                     std::vector<std::string> &values);


        template<class Result>
        void C_SendInternalTxSliceReadKeysResult(Result opResult, int srcPartitionId, int srcDCId) {


            if (srcDCId == _replicaId) //The request originates from a partition in the local data center
            {
                assert(std::find(_myLocalPartitionsIDs.begin(), _myLocalPartitionsIDs.end(), srcPartitionId) !=
                       _myLocalPartitionsIDs.end());
                _partitionClients[srcPartitionId]->SendInternalTxSliceReadKeysResult(opResult);

            } else // the request is from a remote DC that is using this partition as a remote one
            {
                assert(std::find(_currentPartition.adopteeNodesIndexes.begin(),
                                 _currentPartition.adopteeNodesIndexes.end(),
                                 std::make_pair(srcPartitionId, srcDCId)) !=
                       _currentPartition.adopteeNodesIndexes.end());
                _adopteePartitionClients[srcPartitionId][srcDCId]->SendInternalTxSliceReadKeysResult(opResult);
            }
        }

        void processReadRequestsReplies(std::unordered_map<int, TxReadSlice *> &txPartitionToReadSliceMap,
                                        vector<string, allocator<string>> &valueSet);

        /* ************************           Commit related functions          ************************ */

        bool TxCommit(TxConsistencyMetadata &cdata,
                      const std::vector<std::string> &writeKeySet,
                      const std::vector<std::string> &writeValueSet);

        /* Prepare Phase */

        void mapKeysToPartitionInCommitPhase(const vector<string, allocator<string>> &writeKeySet,
                                             const vector<string, allocator<string>> &writeValueSet, Transaction *tx);


        PhysicalTimeSpec LocalPrepareRequest(int txId, TxContex &cdata, const std::vector<std::string> &keys,
                                             std::vector<std::string> &values);

        void sendPrepareRequestsToOtherPartitions(std::unordered_map<int, PrepareRequest *> &txPartToPrepReqMap);

        void prepareLocalKeys(Transaction *tx);

        void InternalPrepareRequest(int txId, TxContex &cdata, const std::vector<std::string> &keys,
                                    std::vector<std::string> &values, int srcPartitionId, int srcDCId);

        void HandleInternalPrepareReply(unsigned int id, int srcPartition, PhysicalTimeSpec pt, double blockDuration);

        PhysicalTimeSpec &processPrepareRequestsReplies(const Transaction *tx);

        /* Commit phase */

        void
        sendCommitRequests(PhysicalTimeSpec commitTime, std::unordered_map<int, PrepareRequest *> &txPartToPrepReqMap);

        void LocalCommitRequest(unsigned int txId, PhysicalTimeSpec ct);

        void HandleInternalCommitRequest(unsigned int txId, PhysicalTimeSpec ct);

        /* ************************     Replication related functions           ************************ */

        PhysicalTimeSpec calculateUpdateBatchTime();

        void PersistAndPropagateUpdates();


        bool HandlePropagatedUpdate(std::vector<PropagatedUpdate *> &updates);

        bool HandleHeartbeat(Heartbeat &hb, int srcReplica);

        /* ************************   Stabilisation protocol related functions  ************************ */

        void launchPartitionStabilizationProtocolThread();

        void launchUniversalTimeStabilizationProtocolThread();

        void UpdateUST();

        void SendGSTAndUSTAtRoot();


        /* Tree */

        void BuildPartialReplicationGSTTree();

        void BuildGSTTree();

        void HandleGSTFromParent(PhysicalTimeSpec parentGST);

        void HandleLSTFromChildren(PhysicalTimeSpec lst, int round);

        /* Broadcast */

        PhysicalTimeSpec getGST();

        PhysicalTimeSpec getUST();


        /* ************************               Utility functions             ************************ */



        void updateClock(PhysicalTimeSpec time);

        PhysicalTimeSpec updateClockOnPrepare(PhysicalTimeSpec time);

        void updateClockOnCommit(PhysicalTimeSpec time);

        uint64_t generateTxIDandAddTxInActiveTxs(PhysicalTimeSpec snapshot);


        /* ************************************************************************************************* */
        /* ************************************************************************************************* */

        boost::shared_mutex mtx;
        typedef boost::shared_lock <boost::shared_mutex> Shared;
        typedef boost::unique_lock <boost::shared_mutex> Exclusive;


    private:
        DBPartition _currentPartition;
        int _partitionId;
        int _replicaId;
        int _totalNumPreloadedKeys;
        bool _isDistributed;
        GroupClient *_groupClient;
        std::vector<std::vector<DBPartition>> _allPartitions;
        std::vector<DBPartition> _myLocalPartitions;
        std::vector<DBPartition> _myRemotePartitions;
        std::vector<DBPartition> _usedByPartitions;
        std::vector<int> _myLocalPartitionsIDs;
        std::vector<int> _myRemotePartitionsIDs;

        std::vector<DBPartition> _myReplicas;
        std::unordered_map<int, int> _ridToIndex;
        std::vector<bool> _partitionLocality;

        int _numPartitions;
        int _numPartitionsInLocalDC;
        int _numLocalPartitions; // does not include the local partition
        int _numRemotePartitions;
        int _numDataCenters;
        int _partitionReplicationFactor;
        bool _readyToServeRequests;
        std::unordered_map<int, ReplicationKVClient *> _replicationClients;
        std::unordered_map<int, ReplicationKVClient *> _stabilizationClients;
        std::unordered_map<int, PartitionKVClient *> _partitionClients;
        std::unordered_map<int, PartitionKVClient *> _partitionWritesClients;
        std::unordered_map<int, std::unordered_map<int, PartitionKVClient *>> _adopteePartitionClients;
        std::unordered_map<int, std::unordered_map<int, PartitionKVClient *>> _adopteePartitionWritesClients;

        ServerMetadata _sdata;

        std::atomic<unsigned long> _tx_counter;
        unsigned long _tx_id;
        volatile unsigned int _tx_lock;
        std::mutex _tx_mutex;
        std::unordered_map<unsigned long, Transaction *> _active_txs;

        std::mutex preparedRequestsMutex;
        std::unordered_map<unsigned int, PrepareRequest *> txIdToPreparedRequestsMap;
        std::set<PrepareRequestElement *, preparedRequestsElementComparator> preparedReqElSet;

        std::mutex commitRequestsMutex;
        WaitHandle _commitReqAvailable;
        std::set<PrepareRequest *, commitRequestsComparator> commitRequests;


        PhysicalTimeSpec HybridClock;
        PhysicalTimeSpec prevHybridClockValue;
        std::mutex _hlc_mutex;

        TreeNode *_treeRoot;
        TreeNode *_currentTreeNode; // current partition node in the tree
        PhysicalTimeSpec _delta;


        // GST round number
        int _gstRoundNum;
        int _recvGSTRoundNum;

        std::unordered_map<int, int> _receivedLSTCounts;
        std::unordered_map<int, PhysicalTimeSpec> _minLSTs;
        std::mutex _minLSTMutex;

        std::unordered_map<int, int> _receivedLSTCountsBC;
        std::unordered_map<int, PhysicalTimeSpec> _minLSTsBC;
        std::mutex _minLSTMutexBC;

        // send gst
        WaitHandle _sendGSTEvent;

        /* for debugging purposes */
        PhysicalTimeSpec ubPrevValue;
        int prevUBCase;
        PhysicalTimeSpec hbPhysTimePrevValue;
        PhysicalTimeSpec vvPrevValue;
        PhysicalTimeSpec prevLocTxReqTS;
        PhysicalTimeSpec latestCommitTime;
        std::mutex commitMutex;
        std::mutex prepareTimeMutex;

        int64_t prevCommitReqTxId;
        PrepareRequest prevCommitReq;

    };

    enum class TreeNodeType {
        RootNode = 1,
        InternalNode = 2,
        LeafNode = 3
    };

    class TreeNode {
    public:
        std::string Name;
        int PartitionId;
        TreeNodeType NodeType;
        PartitionKVClient *PartitionClient;

        TreeNode *Parent;
        std::vector<TreeNode *> Children;
    };

} // namespace scc

#endif
