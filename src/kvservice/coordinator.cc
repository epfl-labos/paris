#include "kvservice/coordinator.h"
#include "kvstore/mv_kvstore.h"
#include "kvstore/log_manager.h"
#include "common/sys_logger.h"
#include "common/utils.h"
#include "common/types.h"
#include "common/sys_stats.h"
#include "common/sys_config.h"
#include <thread>
#include <unordered_map>
#include <queue>
#include <pthread.h>
#include <sched.h>
#include <iostream>
#include "messages/tx_messages.pb.h"
#include "coordinator.h"


#ifndef LOCAL_LINEARIZABLE
#error LOCAL_LINEARIZABLE MUST BE TURNED ON
#endif


namespace scc {

    // both times must be in 64 but format

    double getHybridToPhysicalClockDifferenceInMilliSecs(PhysicalTimeSpec hybridTime,
                                                         PhysicalTimeSpec physicalTime, bool &positive) {

        PhysicalTimeSpec physToHybridTime;

        SET_HYB_PHYS(physToHybridTime.Seconds, physicalTime.Seconds);
        SET_HYB_LOG(physToHybridTime.Seconds, 0);

        PhysicalTimeSpec diff;

        if (hybridTime > physToHybridTime) {
            diff = hybridTime - physToHybridTime;
            positive = true;
        } else {
            diff = physToHybridTime - hybridTime;
            positive = false;
        }
        FROM_64(diff, diff);

        return diff.toMilliSeconds();

    }

    CoordinatorTx::CoordinatorTx(std::string name, int publicPort, int totalNumKeys)
            : _currentPartition(name, publicPort),
              _partitionId(0),
              _replicaId(0),
              _totalNumPreloadedKeys(totalNumKeys),
              _isDistributed(false),
              _groupClient(NULL),
              _readyToServeRequests(false),
              _delta(0, 0),
              _tx_id(0),
              _tx_lock(0),
              _tx_counter(0) {

        TO_64(_delta, _delta);

        //Convert physical 64 bit to hybrid
        SET_HYB_PHYS(_delta.Seconds, _delta.Seconds);
        SET_HYB_LOG(_delta.Seconds, 0);

    }

    CoordinatorTx::CoordinatorTx(std::string serverName,
                                 int publicPort,
                                 int partitionPort,
                                 int replicationPort,
                                 int partitionId,
                                 int replicaId,
                                 int totalNumKeys,
                                 std::string groupServerName,
                                 int groupServerPort,
                                 int replicationFactor)
            : _currentPartition(serverName, publicPort, partitionPort,
                                replicationPort, partitionId, replicaId, replicationFactor),
              _partitionId(partitionId),
              _replicaId(replicaId),
              _totalNumPreloadedKeys(totalNumKeys),
              _isDistributed(true),
              _groupClient(NULL),
              _readyToServeRequests(false),
              _delta(0, 0),
              _tx_id(0),
              _tx_lock(0),
              _tx_counter(0),
              _partitionReplicationFactor(replicationFactor) {
        _groupClient = new GroupClient(groupServerName, groupServerPort);
        TO_64(_delta, _delta);

        //Convert physical 64 bit to hybrid
        SET_HYB_PHYS(_delta.Seconds, _delta.Seconds);
        SET_HYB_LOG(_delta.Seconds, 0);

    }

    CoordinatorTx::~CoordinatorTx() {
        if (_isDistributed) {
            delete _groupClient;

            for (unsigned int i = 0; i < _myReplicas.size(); i++) {
                delete _replicationClients[_myReplicas[i].ReplicaId];
            }

            for (unsigned int i = 0; i < _myLocalPartitions.size(); i++) {
                delete _partitionClients[_myLocalPartitions[i].PartitionId];
                delete _partitionWritesClients[_myLocalPartitions[i].PartitionId];
            }

        }
    }

    /* initialization functions */

    void CoordinatorTx::Initialize() {
        ItemAnchor::_replicaId = _currentPartition.ReplicaId;

        if (!_isDistributed) {
            // load keys
            loadKeys();

            _readyToServeRequests = true;
        } else {
            // register at the group manager
            _groupClient->RegisterPartition(_currentPartition);

            // get all partitions in the system
            _allPartitions = _groupClient->GetRegisteredPartitions();
            _currentPartition.NodeId = _allPartitions[_currentPartition.PartitionId][_currentPartition.ReplicaId].NodeId;


            _currentPartition.adopteeNodesIndexes = _groupClient->GetPartitionUsersIds(_currentPartition.PartitionId,
                                                                                       _currentPartition.ReplicaId);

            _numPartitions = _allPartitions.size();
            _tx_counter = _currentPartition.NodeId;
            _numDataCenters = _allPartitions[0].size();

            _partitionLocality.resize(_numPartitions);

            // initialize _myReplicas, _myLocalPartitions and _myRemotePartitions from _allPartitions
            initializeReplicasAndPartitions();

            _ridToIndex[_currentPartition.ReplicaId] = 0;
            for (int i = 0; i < _myReplicas.size(); i++) {
                _ridToIndex[_myReplicas[i].ReplicaId] = i + 1;
            }

            // initialize Log Manager
            LogManager::Instance()->Initialize(_partitionReplicationFactor);

            // initialize MVKVTXStore
            MVKVTXStore::Instance()->SetPartitionInfo(_currentPartition, _numPartitions, _numPartitionsInLocalDC,
                                                      _partitionReplicationFactor, _numDataCenters, _ridToIndex,
                                                      _myLocalPartitionsIDs);

            MVKVTXStore::Instance()->Initialize();
            SLOG("[INFO]: MVKVTXStore initialized.\n");

            // load keys 
            loadKeys();

            // connect to remote replicas
            if (_partitionReplicationFactor > 1) {
                connectToRemoteReplicas();
            }

            // connect to local partitions
            if (_myLocalPartitions.size() >= 1) {
                connectToLocalPartitions();
            }

            if (_myRemotePartitions.size() >= 1) {
                connectToRemotePartitions();
            }

            if (_usedByPartitions.size() >= 1) {
                connectToRemoteAdoptivePartitions();
            }

            PhysicalTimeSpec now = Utils::getCurrentClockTimeIn64bitFormat();

            SET_HYB_PHYS(HybridClock.Seconds, now.Seconds);
            SET_HYB_LOG(HybridClock.Seconds, 0);

            _sdata.GST.setToZero();

#if !defined(BLOCKING_ON) && !defined(EVENTUAL_CONSISTENCY)
            if (_numPartitions > 1) {
                launchPartitionStabilizationProtocolThread();
            }
#endif


            SLOG("[INFO]: Launch update persistence and replication thread");
            if (_partitionReplicationFactor > 1) {
                std::thread t(&CoordinatorTx::PersistAndPropagateUpdates, this);
                t.detach();
            }

            // I'm ready! Notify the group manager
            _groupClient->NotifyReadiness(_currentPartition);

            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            _readyToServeRequests = true;

            prevUBCase = 0;

            SLOG("[INFO]: Ready to serve client requests.");
        }
    }

    /* action functions */
    void CoordinatorTx::Echo(const std::string &input,
                             std::string &output) {
        output = input;
    }

    bool CoordinatorTx::Add(const std::string &key, const std::string &value) {
        return MVKVTXStore::Instance()->Add(key, value);
    }

    bool CoordinatorTx::ShowItem(const std::string &key, std::string &itemVersions) {
        int partitionId = std::stoi(key) % _numPartitions;
        if (partitionId == _currentPartition.PartitionId) {
            return MVKVTXStore::Instance()->ShowItem(key, itemVersions);
        } else {
            return _partitionClients[partitionId]->ShowItem(key, itemVersions);
        }
    }

    bool CoordinatorTx::ShowDB(std::string &allItemVersions) {
        return MVKVTXStore::Instance()->ShowDB(allItemVersions);
    }

    bool CoordinatorTx::ShowState(std::string &stateStr) {
        return MVKVTXStore::Instance()->ShowStateTx(stateStr);
    }

    bool CoordinatorTx::ShowStateCSV(std::string &stateStr) {
        return MVKVTXStore::Instance()->ShowStateTxCSV(stateStr);
    }

    bool CoordinatorTx::DumpLatencyMeasurement(std::string &resultStr) {
        return MVKVTXStore::Instance()->DumpLatencyMeasurement(resultStr);
    }

    Transaction *CoordinatorTx::GetTransaction(unsigned long id) {

        Transaction *ret;
        LOCK_XACT();
            assert(_active_txs.find(id) != _active_txs.end());

            ret = _active_txs[id];
        UNLOCK_XACT();
        assert(ret != NULL);

        return ret;
    }

    void CoordinatorTx::initializeReplicasAndPartitions() {
        for (std::pair<int, int> const coord: _currentPartition.adopteeNodesIndexes) {
            DBPartition dbp = _allPartitions[coord.first][coord.second];
            _usedByPartitions.push_back(dbp);
        }

        for (int i = 0; i < _numDataCenters; i++) {
            DBPartition dbp = _allPartitions[_currentPartition.PartitionId][i];
            if (dbp.ReplicaId != _currentPartition.ReplicaId) {
                if (std::find(_myReplicas.begin(), _myReplicas.end(), dbp) == _myReplicas.end())
                    _myReplicas.push_back(dbp);
            }
        }

        for (int i = 0; i < _numPartitions; i++) {
            if (i != _currentPartition.PartitionId) {

                DBPartition dbp = _allPartitions[i][_currentPartition.ReplicaId];

                if (dbp.ReplicaId == _currentPartition.ReplicaId) {
                    dbp.isLocal = true;
                    _myLocalPartitionsIDs.push_back(dbp.PartitionId);
                    _myLocalPartitions.push_back(dbp);
                    _partitionLocality[dbp.PartitionId] = true;

                } else {
                    dbp.isLocal = false;
                    _partitionLocality[dbp.PartitionId] = false;
                    _myRemotePartitions.push_back(dbp);
                    _myRemotePartitionsIDs.push_back(dbp.PartitionId);
                }
            }
        }
        _numLocalPartitions = _myLocalPartitions.size();
        _numPartitionsInLocalDC = _numLocalPartitions + 1;
        _numRemotePartitions = _myRemotePartitions.size();

        assert(_myLocalPartitionsIDs.size() == _myLocalPartitions.size());
        assert(_myRemotePartitionsIDs.size() == _myRemotePartitions.size());
        assert(_numLocalPartitions + _numRemotePartitions ==
               (_numPartitions - 1));  //the current partition is not included in its local partitions

    }


    void CoordinatorTx::loadKeys() const {

        for (int i = 0; i < _totalNumPreloadedKeys; i++) {

            string key = to_string(i);
            string value(8, 'x'); // default value size is hard coded

            if (!_isDistributed) {
                MVKVTXStore::Instance()->Add(key, value);
            } else {
                if (i % _numPartitions == _currentPartition.PartitionId) {
                    MVKVTXStore::Instance()->Add(key, value);
                }
            }
        }

        int keyCount = MVKVTXStore::Instance()->Size();
        SLOG((boost::format("[INFO]: Loaded all %d keys.") % keyCount).str());
    }

    void CoordinatorTx::connectToRemoteReplicas() {

        for (unsigned int i = 0; i < _myReplicas.size(); i++) {
            DBPartition &p = _myReplicas[i];
            ReplicationKVClient *client = new ReplicationKVClient(p.Name, p.ReplicationPort, p.ReplicaId);
            _replicationClients[p.ReplicaId] = client;
        }

        SLOG((boost::format("[INFO]: Connected to %d remote replicas.") % _myReplicas.size()).str());

    }

    void CoordinatorTx::connectToStabilizationPeers() {

        int repCounter = 0;
        int partitionId = 0;
        int dcId = 0;

        while (dcId < _numDataCenters) {

            if (dcId != _currentPartition.ReplicaId || partitionId != _currentPartition.PartitionId) {
                DBPartition &p = _allPartitions[partitionId][dcId];
                ReplicationKVClient *client = new ReplicationKVClient(p.Name, p.ReplicationPort, p.ReplicaId);
                _stabilizationClients[p.ReplicaId] = client;
            }


            if (repCounter == (_partitionReplicationFactor - 1)) {
                repCounter == 0;
                partitionId++;
            }

            repCounter++;
            dcId++;
        }

        assert(_stabilizationClients.size() == (_numDataCenters - 1));

        SLOG((boost::format("[INFO]: Connected to %d stabilization peers.") % _stabilizationClients.size()).str());

    }


    void CoordinatorTx::connectToLocalPartitions() {
        for (unsigned int i = 0; i < _myLocalPartitions.size(); i++) {
            DBPartition &p = _myLocalPartitions[i];

            PartitionKVClient *client = new PartitionKVClient(p.Name, p.PartitionPort);
            _partitionClients[p.PartitionId] = client;

            PartitionKVClient *clientWrites = new PartitionKVClient(p.Name, p.PartitionPort + 100);
            _partitionWritesClients[p.PartitionId] = clientWrites;
        }


        this_thread::sleep_for(milliseconds(500));
        SLOG((boost::format("[INFO]: Connected to %d local partitions.") % _myLocalPartitions.size()).str());
    }

    void CoordinatorTx::connectToRemotePartitions() {
        for (unsigned int i = 0; i < _myRemotePartitions.size(); i++) {
            DBPartition &p = _myRemotePartitions[i];

            PartitionKVClient *client = new PartitionKVClient(p.Name, p.PartitionPort);
            _partitionClients[p.PartitionId] = client;

            PartitionKVClient *clientWrites = new PartitionKVClient(p.Name, p.PartitionPort + 100);
            _partitionWritesClients[p.PartitionId] = clientWrites;
        }

        this_thread::sleep_for(milliseconds(500));
        SLOG((boost::format("[INFO]: Connected to %d remote partitions.") % _myRemotePartitions.size()).str());
    }

    void CoordinatorTx::connectToRemoteAdoptivePartitions() {
        assert(_usedByPartitions.size() == _currentPartition.adopteeNodesIndexes.size());

        for (unsigned int i = 0; i < _usedByPartitions.size(); i++) {
            DBPartition &p = _usedByPartitions[i];

            PartitionKVClient *client = new PartitionKVClient(p.Name, p.PartitionPort);
            _adopteePartitionClients[p.PartitionId][p.ReplicaId] = client;

            PartitionKVClient *clientWrites = new PartitionKVClient(p.Name, p.PartitionPort + 100);
            _adopteePartitionWritesClients[p.PartitionId][p.ReplicaId] = clientWrites;
        }

        this_thread::sleep_for(milliseconds(500));
        SLOG((boost::format("[INFO]: Connected to %d adoptee partitions.") % _usedByPartitions.size()).str());
    }


/* ************************   Start of transaction related functions    ************************ */

    uint64_t CoordinatorTx::generateTxIDandAddTxInActiveTxs(PhysicalTimeSpec snapshot) {
        unsigned long tid;

        LOCK_XACT();
            tid = getAndIncrementTxCount();

            assert(_active_txs.find(tid) == _active_txs.end());

            _active_txs[tid] = new Transaction(tid, new TxContex(snapshot));

            assert(_active_txs.find(tid) != _active_txs.end());

        UNLOCK_XACT();

        return tid;
    }

    bool CoordinatorTx::TxStart(TxConsistencyMetadata &cdata) {

#ifndef EVENTUAL_CONSISTENCY

        MVKVTXStore::Instance()->updateUSTifSmaller(cdata.UST);

#ifndef BLOCKING_ON

        cdata.UST = getUST();

#else

        PhysicalTimeSpec clock;
        clock = Utils::getCurrentClockTimeIn64bitFormat();
        cdata.UST = MAX(cdata.UST, clock);

#endif //BLOCKING_ON
#endif //EVENTUAL_CONSISTENCY

        cdata.txId = generateTxIDandAddTxInActiveTxs(cdata.UST);

        return true;
    }


/* ************************    Transactional read related functions     ************************ */

    bool CoordinatorTx::TxRead(int txId, const std::vector<std::string> &keySet,
                               std::vector<std::string> &valueSet) {
#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
        SysStats::NumReadItemsFromKVStore = SysStats::NumReadItemsFromKVStore + keySet.size();
#endif //MEASURE_STATISTICS

        int numReadSlices;

        Transaction *tx = GetTransaction(txId);

#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec startTime1 = Utils::GetCurrentClockTime();
#endif //MEASURE_STATISTICS

        mapKeysToPartitionInReadPhase(keySet, valueSet, tx->partToReadSliceMap);

#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec endTime1 = Utils::GetCurrentClockTime();
        double duration1 = (endTime1 - startTime1).toMilliSeconds();
        SysStats::CoordinatorMapingKeysToShardsLatencySum =
                SysStats::CoordinatorMapingKeysToShardsLatencySum + duration1;
        SysStats::NumCoordinatorMapingKeysToShardsLatencyMeasurements++;

        PhysicalTimeSpec startTime2 = Utils::GetCurrentClockTime();
#endif //MEASURE_STATISTICS

        // Notify readSlicesWaitHandle of how many threads it has to wait
        numReadSlices = tx->partToReadSliceMap.size();
        tx->readSlicesWaitHandle = new WaitHandle(numReadSlices);

        sendTxReadSliceRequestsToOtherPartitions(tx);

#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec endTime2 = Utils::GetCurrentClockTime();
        double duration2 = (endTime2 - startTime2).toMilliSeconds();
        SysStats::CoordinatorSendingReadReqToShardsLatencySum =
                SysStats::CoordinatorSendingReadReqToShardsLatencySum + duration2;
        SysStats::NumCoordinatorSendingReadReqToShardsLatencyMeasurements++;

        PhysicalTimeSpec startTime3 = Utils::GetCurrentClockTime();
#endif //MEASURE_STATISTICS

        readLocalKeys(tx);

#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec endTime3 = Utils::GetCurrentClockTime();
        double duration3 = (endTime3 - startTime3).toMilliSeconds();
        SysStats::CoordinatorReadingLocalKeysLatencySum = SysStats::CoordinatorReadingLocalKeysLatencySum + duration3;
        SysStats::NumCoordinatorReadingLocalKeysLatencyMeasurements++;

        PhysicalTimeSpec startTime4 = Utils::GetCurrentClockTime();
#endif //MEASURE_STATISTICS

        processReadRequestsReplies(tx->partToReadSliceMap, valueSet);

#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec endTime4 = Utils::GetCurrentClockTime();
        double duration4 = (endTime4 - startTime4).toMilliSeconds();
        SysStats::CoordinatorProcessingOtherShardsReadRepliesLatencySum =
                SysStats::CoordinatorProcessingOtherShardsReadRepliesLatencySum + duration4;
        SysStats::NumCoordinatorProcessingOtherShardsReadRepliesLatencyMeasurements++;
#endif //MEASURE_STATISTICS

        for (auto it : tx->partToReadSliceMap) {
            delete it.second;
        }

        tx->partToReadSliceMap.clear();

        //        delete tx->readSlicesWaitHandle;

#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
        double duration = (endTime - startTime).toMilliSeconds();
        SysStats::ServerReadLatencySum = SysStats::ServerReadLatencySum + duration;
        SysStats::NumServerReadLatencyMeasurements++;
#endif //MEASURE_STATISTICS

        return true;
    }

    void CoordinatorTx::mapKeysToPartitionInReadPhase(const vector<string, allocator<string>> &keySet,
                                                      vector<string, allocator<string>> &valueSet,
                                                      std::unordered_map<int, TxReadSlice *> &txPartitionToReadSliceMap) {
        string key;
        int partitionId;

        txPartitionToReadSliceMap.clear();

        for (unsigned int i = 0; i < keySet.size(); ++i) {

            key = keySet[i];
            valueSet.push_back("");
            partitionId = stoi(key) % _numPartitions;

            if (!txPartitionToReadSliceMap.count(partitionId)) {
                //Slice for partitionId does not exists
                txPartitionToReadSliceMap[partitionId] = new TxReadSlice();
            }

            TxReadSlice *slice;
            slice = txPartitionToReadSliceMap[partitionId];
            if (partitionId != _currentPartition.PartitionId) {
                if (std::find(_myLocalPartitionsIDs.begin(), _myLocalPartitionsIDs.end(), partitionId) !=
                    _myLocalPartitionsIDs.end()) {

                } else {
                    assert(std::find(_myRemotePartitionsIDs.begin(), _myRemotePartitionsIDs.end(), partitionId) !=
                           _myRemotePartitionsIDs.end());

                }
            }

            slice->keys.push_back(key);
            slice->positionIds.push_back(i);
        }
    }

    void CoordinatorTx::readLocalKeys(Transaction *tx) {
#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec startTime1 = Utils::GetCurrentClockTime();
        PhysicalTimeSpec startTime2, endTime2;
#endif //MEASURE_STATISTICS

        int localPartitionId = _currentPartition.PartitionId;

        if (tx->partToReadSliceMap.count(localPartitionId) > 0) {
            TxReadSlice *localSlice = tx->partToReadSliceMap[localPartitionId];
            vector<string> keys = localSlice->keys;


            int numKeys = keys.size();
            localSlice->values.resize(numKeys, "");

            InternalTxSliceReadKeys(tx->txId, *(tx->txContex), localSlice->keys, localSlice->values);

            localSlice->txWaitOnReadTime = tx->txContex->waited_xact;

#ifdef MEASURE_STATISTICS
            PhysicalTimeSpec endTime1 = Utils::GetCurrentClockTime();
            double duration1 = (endTime1 - startTime1).toMilliSeconds();
            SysStats::CoordinatorLocalKeyReadsLatencySum =
                    SysStats::CoordinatorLocalKeyReadsLatencySum + duration1;
            SysStats::NumCoordinatorLocalKeyReadsLatencyMeasurements++;

            startTime2 = Utils::GetCurrentClockTime();
#endif //MEASURE_STATISTICS

            tx->readSlicesWaitHandle->DecrementAndWaitIfNonZero();

#ifdef MEASURE_STATISTICS
            endTime2 = Utils::GetCurrentClockTime();
            double duration2 = (endTime2 - startTime2).toMilliSeconds();
            SysStats::CoordinatorWaitingForShardsRepliesLatencySum =
                    SysStats::CoordinatorWaitingForShardsRepliesLatencySum + duration2;
            SysStats::NumCoordinatorWaitingForShardsRepliesMeasurements++;
#endif //MEASURE_STATISTICS

        } else {
#ifdef MEASURE_STATISTICS
            startTime2 = Utils::GetCurrentClockTime();
#endif //MEASURE_STATISTICS

            tx->readSlicesWaitHandle->WaitIfNonZero();

#ifdef MEASURE_STATISTICS
            endTime2 = Utils::GetCurrentClockTime();
            double duration2 = (endTime2 - startTime2).toMilliSeconds();
            SysStats::CoordinatorWaitingForShardsRepliesLatencySum =
                    SysStats::CoordinatorWaitingForShardsRepliesLatencySum + duration2;
            SysStats::NumCoordinatorWaitingForShardsRepliesMeasurements++;
#endif //MEASURE_STATISTICS
        }
    }


    void CoordinatorTx::processReadRequestsReplies(std::unordered_map<int, TxReadSlice *> &txPartitionToReadSliceMap,
                                                   vector<string, allocator<string>> &valueSet) {
        int partitionId;
        double maxWaitingTime = 0;

        for (auto it:txPartitionToReadSliceMap) {
            partitionId = it.first;
            TxReadSlice *slice = it.second;

            for (int i = 0; i < slice->values.size(); i++) {
                int position = slice->positionIds[i];
                valueSet[position] = slice->values[i];
            }

#ifdef BLOCKING_ON
#ifdef MEASURE_STATISTICS
            maxWaitingTime = MAX(maxWaitingTime, slice->txWaitOnReadTime);
            assert(slice->txWaitOnReadTime >= 0);
#endif
#endif

        }

        assert(maxWaitingTime >= 0);

#ifdef MEASURE_STATISTICS
        SysStats::NumTxReadPhases++;
        if (maxWaitingTime > 0) {
            SysStats::TxReadBlockDurationAtCoordinator = SysStats::TxReadBlockDurationAtCoordinator + maxWaitingTime;
            SysStats::NumTxReadBlocksAtCoordinator++;
        }
#endif
    }


    bool
    CoordinatorTx::InternalTxSliceReadKeys(unsigned int txId, TxContex &cdata,
                                           const std::vector<std::string> &keys, std::vector<std::string> &values) {

#ifdef MEASURE_STATISTICS
        SysStats::NumInternalTxSliceReadRequests++;
#endif
        bool ret = true;

#ifndef EVENTUAL_CONSISTENCY
        PhysicalTimeSpec vvValue = MVKVTXStore::Instance()->GetVV(_replicaId);

        MVKVTXStore::Instance()->updateUSTifSmaller(cdata.UST);

#ifdef BLOCKING_ON
        MVKVTXStore::Instance()->WaitOnTxSlice(txId, cdata);
#endif

        assert(cdata.UST <= vvValue);
#endif //EVENTUAL_CONSISTENCY

        for (int i = 0; i < keys.size(); i++) {
            ret = ret && MVKVTXStore::Instance()->LocalTxSliceGet(cdata, keys[i], values[i]);
        }

        return ret;
    }


    void CoordinatorTx::sendTxReadSliceRequestsToOtherPartitions(Transaction *tx) {
        int partitionId;

        int localPartitionId = _currentPartition.PartitionId;
        int localDataCenterId = _currentPartition.ReplicaId;

        for (auto it : tx->partToReadSliceMap) {
            partitionId = it.first;
            if (partitionId != localPartitionId) {
                vector<string> keys = it.second->keys;

                assert(_partitionClients.find(partitionId) != _partitionClients.end());

                _partitionClients[partitionId]->TxSliceReadKeys(tx->txId, *(tx->txContex), keys, localPartitionId,
                                                                localDataCenterId);
            }
        }
    }


    void CoordinatorTx::launchPartitionStabilizationProtocolThread() {
        if (SysConfig::GSTDerivationMode == GSTDerivationType::TREE) {
            // build GST tree
            BuildPartialReplicationGSTTree();

            if (_currentTreeNode->NodeType == TreeNodeType::RootNode && _numDataCenters > 1) {
                connectToStabilizationPeers();
            }

            if (_currentTreeNode->NodeType == TreeNodeType::RootNode) {
                {
                    // launch global stabilization thread if current node is root
                    thread t(&CoordinatorTx::SendGSTAndUSTAtRoot, this);

                    sched_param sch;
                    int policy;
                    pthread_getschedparam(t.native_handle(), &policy, &sch);
                    sch.sched_priority = sched_get_priority_max(SCHED_FIFO);
                    if (pthread_setschedparam(t.native_handle(), SCHED_FIFO, &sch)) {
                        cout << "Failed to setschedparam: " << strerror(errno) << '\n';
                    }

                    t.detach();
                }
                //only the root runs the UST protocols among DCs
                launchUniversalTimeStabilizationProtocolThread();

            }

        } else {
            cout << "Nonexistent/Unsupported GSTDerivationType: \n";
            assert(false);
        }
        SLOG("[INFO]: GST Partition stabilization protocol thread scheduled\n");
    }

    void CoordinatorTx::launchUniversalTimeStabilizationProtocolThread() {
        std::thread t_ust(&CoordinatorTx::UpdateUST, this);

        sched_param sch;
        int policy;
        pthread_getschedparam(t_ust.native_handle(), &policy, &sch);
        sch.sched_priority = sched_get_priority_max(SCHED_FIFO);
        if (pthread_setschedparam(t_ust.native_handle(), SCHED_FIFO, &sch)) {
            std::cout << "Failed to setschedparam: " << strerror(errno) << '\n';
        }
        t_ust.detach();
        fprintf(stdout, "UST update thread scheduled\n");
    }

    void CoordinatorTx::UpdateUST() {

        // wait until all replicas are ready
        while (!_readyToServeRequests) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        while (true) {
            std::this_thread::sleep_for(std::chrono::microseconds(SysConfig::GSTComputationInterval));
            MVKVTXStore::Instance()->UpdateUST();
        }
    }

    void CoordinatorTx::pushGlobalStabilizationTimeToDCPeers(PhysicalTimeSpec gst) {

        for (auto peerEntry: _stabilizationClients) {
            peerEntry.second->PushGST(gst, _replicaId);
        }
    }

    void CoordinatorTx::HandleGSTPush(PhysicalTimeSpec gst, int srcDC) {

        MVKVTXStore::Instance()->HandleGSTPushFromPeerDC(gst, srcDC);
    }

    void CoordinatorTx::SendGSTAndUSTAtRoot() {
        // wait until all replicas are ready

        while (!_readyToServeRequests) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        while (true) {
            std::this_thread::sleep_for(std::chrono::microseconds(SysConfig::GSTComputationInterval));


            if (_numPartitions == 1) {

                //TODO:: FIXME

//                PhysicalTimeSpec global;
//                MVKVTXStore::Instance()->getCurrentGlobalValue(global);
//                setGST(global);

            } else {

                PhysicalTimeSpec gst, ust;

                gst = getGST();
                ust = MVKVTXStore::Instance()->getUST();

                for (unsigned int i = 0; i < _currentTreeNode->Children.size(); ++i) {
                    _currentTreeNode->Children[i]->PartitionClient->SendGSTAndUST(gst, ust);
                }
            }
        }
    }


    bool CoordinatorTx::TxCommit(TxConsistencyMetadata &cdata,
                                 const std::vector<std::string> &writeKeySet,
                                 const std::vector<std::string> &writeValueSet) {

#ifdef MEASURE_STATISTICS
        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();
#endif

        Transaction *tx = GetTransaction(cdata.txId);

        if (!writeKeySet.empty()) {

            PhysicalTimeSpec startTime1 = Utils::GetCurrentClockTime();

#ifndef EVENTUAL_CONSISTENCY
            PhysicalTimeSpec ht = MAX(tx->txContex->UST, cdata.CT);
            tx->txContex->HT = ht;
#else
            tx->txContex->HT = cdata.CT;

#endif //EVENTUAL_CONSISTENCY

            int localPartitionId = _currentPartition.PartitionId;
            int numPrepareMsgs;
            std::string key, val;
            PhysicalTimeSpec commitTime;

            mapKeysToPartitionInCommitPhase(writeKeySet, writeValueSet, tx);

#ifdef MEASURE_STATISTICS
            PhysicalTimeSpec endTime1 = Utils::GetCurrentClockTime();
            double duration1 = (endTime1 - startTime1).toMilliSeconds();
            SysStats::CoordinatorCommitMappingKeysToShardsLatencySum =
                    SysStats::CoordinatorCommitMappingKeysToShardsLatencySum + duration1;
            SysStats::NumCoordinatorCommitMappingKeysToShardsLatencyMeasurements++;

            startTime1 = Utils::GetCurrentClockTime();
#endif

            int partitionId;

            numPrepareMsgs = tx->partToPrepReqMap.size();

            // Notify prepareRequestWaitHandle of how many threads it has to wait
            tx->prepareReqWaitHandle = new WaitHandle(numPrepareMsgs);

            sendPrepareRequestsToOtherPartitions(tx->partToPrepReqMap);

#ifdef MEASURE_STATISTICS
            endTime1 = Utils::GetCurrentClockTime();
            duration1 = (endTime1 - startTime1).toMilliSeconds();
            SysStats::CoordinatorCommitSendingPrepareReqToShardsLatencySum =
                    SysStats::CoordinatorCommitSendingPrepareReqToShardsLatencySum + duration1;
            SysStats::NumCoordinatorCommitSendingPrepareReqToShardsLatencyMeasurements++;

            startTime1 = Utils::GetCurrentClockTime();
#endif
            int localCount = tx->partToPrepReqMap.count(localPartitionId);

            //In case you have keys to write to the local partition
            if (localCount > 0) {
                prepareLocalKeys(tx);
            }

#ifdef MEASURE_STATISTICS
            endTime1 = Utils::GetCurrentClockTime();
            duration1 = (endTime1 - startTime1).toMilliSeconds();
            SysStats::CoordinatorCommitLocalPrepareLatencySum =
                    SysStats::CoordinatorCommitLocalPrepareLatencySum + duration1;
            SysStats::NumCoordinatorCommitLocalPrepareLatencyMeasurements++;

            startTime1 = Utils::GetCurrentClockTime();
#endif

            if (localCount > 0) {
                tx->prepareReqWaitHandle->DecrementAndWaitIfNonZero();
            } else {
                tx->prepareReqWaitHandle->WaitIfNonZero();
            }

#ifdef MEASURE_STATISTICS
            endTime1 = Utils::GetCurrentClockTime();
            duration1 = (endTime1 - startTime1).toMilliSeconds();
            SysStats::CoordinatorCommitWaitingOtherShardsPrepareRepliesLatencySum =
                    SysStats::CoordinatorCommitWaitingOtherShardsPrepareRepliesLatencySum + duration1;
            SysStats::NumCoordinatorCommitWaitingOtherShardsPrepareRepliesLatencyMeasurements++;
            startTime1 = Utils::GetCurrentClockTime();
#endif

            commitTime = processPrepareRequestsReplies(tx);
            assert(commitTime.Seconds > 0);

            cdata.CT = commitTime;

#ifdef MEASURE_STATISTICS
            endTime1 = Utils::GetCurrentClockTime();
            duration1 = (endTime1 - startTime1).toMilliSeconds();
            SysStats::CoordinatorCommitProcessingPrepareRepliesLatencySum =
                    SysStats::CoordinatorCommitProcessingPrepareRepliesLatencySum + duration1;
            SysStats::NumCoordinatorCommitProcessingPrepareRepliesLatencyMeasurements++;

            startTime1 = Utils::GetCurrentClockTime();
#endif

            sendCommitRequests(cdata.CT, tx->partToPrepReqMap);

#ifdef MEASURE_STATISTICS
            endTime1 = Utils::GetCurrentClockTime();
            duration1 = (endTime1 - startTime1).toMilliSeconds();
            SysStats::CoordinatorCommitSendingCommitReqLatencySum =
                    SysStats::CoordinatorCommitSendingCommitReqLatencySum + duration1;
            SysStats::NumCoordinatorCommitSendingCommitReqLatencyMeasurements++;
#endif
        }

        LOCK_XACT();
            _active_txs.erase(tx->txId);
            assert(_active_txs.find(tx->txId) == _active_txs.end());
        UNLOCK_XACT();
        delete tx;

#ifdef MEASURE_STATISTICS
        SysStats::NumCommitedTxs++;

        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();
        double duration = (endTime - startTime).toMilliSeconds();
        SysStats::CoordinatorCommitLatencySum = SysStats::CoordinatorCommitLatencySum + duration;
        SysStats::NumCoordinatorCommitMeasurements++;
#endif

        return true;
    }


    void CoordinatorTx::sendCommitRequests(PhysicalTimeSpec commitTime,
                                           std::unordered_map<int, PrepareRequest *> &txPartToPrepReqMap) {
        int partitionId;
        int localPartitionId = _currentPartition.PartitionId;

        for (
            auto it
                :
                txPartToPrepReqMap) {
            partitionId = it.first;
            PrepareRequest *pReqTemp = it.second;

            if (partitionId != localPartitionId) {
                assert(_partitionWritesClients.find(partitionId) != _partitionWritesClients.end());

                _partitionWritesClients[partitionId]->CommitRequest(pReqTemp->id, commitTime);
            } else {
                LocalCommitRequest(pReqTemp->id, commitTime);
            }
        }
    }

    PhysicalTimeSpec &CoordinatorTx::processPrepareRequestsReplies(const Transaction *tx) {
        double maxWaitingTime = 0.0;
        PhysicalTimeSpec commitTime;

        for (auto it: tx->partToPrepReqMap) {
            PhysicalTimeSpec prepareTime = it.second->prepareTime;
            commitTime = MAX(commitTime, prepareTime);
#ifdef MEASURE_STATISTICS
            maxWaitingTime = MAX(maxWaitingTime, it.second->prepareBlockDuration);
#endif
        }

#ifdef MEASURE_STATISTICS
        if (maxWaitingTime > 0) {
            SysStats::TxPreparePhaseBlockDurationAtCoordinator =
                    SysStats::TxPreparePhaseBlockDurationAtCoordinator + (double) maxWaitingTime;
            SysStats::NumTxPreparePhaseBlocksAtCoordinator++;
        }
#endif
        return commitTime;
    }

    void CoordinatorTx::prepareLocalKeys(Transaction *tx) {
        int localPartitionId = _currentPartition.PartitionId;

        PrepareRequest *localPrepReq = tx->partToPrepReqMap[localPartitionId];
        localPrepReq->prepareTime = LocalPrepareRequest(tx->txId, *(tx->txContex),
                                                        localPrepReq->keys,
                                                        localPrepReq->values);
        localPrepReq->prepareBlockDuration = tx->txContex->timeWaitedOnPrepareXact;
    }

    void CoordinatorTx::mapKeysToPartitionInCommitPhase(const vector<string, allocator<string>> &writeKeySet,
                                                        const vector<string, allocator<string>> &writeValueSet,
                                                        Transaction *tx) {

        int partitionId;
        std::string key, val;

        for (auto it : tx->partToPrepReqMap) {
            delete it.second;
        }

        tx->partToPrepReqMap.clear();

        // Create helper maps that map keys and values to partitions
        for (unsigned int i = 0; i < writeKeySet.size(); ++i) {
            key = writeKeySet[i];
            val = writeValueSet[i];
            partitionId = std::stoi(key) % _numPartitions;
            if (!tx->partToPrepReqMap.count(partitionId)) {
                tx->partToPrepReqMap[partitionId] = new PrepareRequest(tx->txId, *(tx->txContex));

            }

            PrepareRequest *prepareRequest;
            prepareRequest = tx->partToPrepReqMap[partitionId];

            assert(prepareRequest->id == tx->txId);

            prepareRequest->keys.push_back(key);
            prepareRequest->values.push_back(val);
        }
    }

    void
    CoordinatorTx::sendPrepareRequestsToOtherPartitions(
            std::unordered_map<int, PrepareRequest *> &txPartToPrepReqMap) {
        int partitionId;
        int localPartitionId = _currentPartition.PartitionId;
        int localDataCenterId = _currentPartition.ReplicaId;

        for (auto it : txPartToPrepReqMap) {
            partitionId = it.first;
            PrepareRequest *tempReq = it.second;
            if (partitionId != localPartitionId) {
                std::vector<std::string> keys = tempReq->keys;
                std::vector<std::string> vals = tempReq->values;

                assert(_partitionWritesClients.find(partitionId) != _partitionWritesClients.end());

                _partitionWritesClients[partitionId]->PrepareRequest(tempReq->id, tempReq->metadata, keys, vals,
                                                                     localPartitionId, localDataCenterId);
            }
        }
    }


    void CoordinatorTx::LocalCommitRequest(unsigned int txId, PhysicalTimeSpec ct) {
        LOCK_PREPARED_REQUESTS();

#ifndef EVENTUAL_CONSISTENCY
            updateClockOnCommit(ct);
#endif //EVENTUAL_CONSISTENCY

            assert(txIdToPreparedRequestsMap.find(txId) != txIdToPreparedRequestsMap.end());

            PrepareRequest *req = txIdToPreparedRequestsMap[txId];
            assert(req != NULL);
            assert(req->id == txId);

            req->commitTime = ct;

            PrepareRequestElement *pRequestEl = new PrepareRequestElement(req->id, req->prepareTime);

#ifdef PROFILE_STATS
            req->metadata.endTimePreparedSet = Utils::GetCurrentClockTime();
            double duration = (req->metadata.endTimePreparedSet - req->metadata.startTimePreparedSet).toMilliSeconds();

            {
                std::lock_guard<std::mutex> lk(SysStats::timeInPreparedSet.valuesMutex);
                SysStats::timeInPreparedSet.values.push_back(duration);
            }
#endif

            assert(txIdToPreparedRequestsMap.find(txId) != txIdToPreparedRequestsMap.end());
            //            delete txIdToPreparedRequestsMap[txId];
            txIdToPreparedRequestsMap.erase(txId);
            assert(txIdToPreparedRequestsMap.find(txId) == txIdToPreparedRequestsMap.end());

            int pReqElSize = preparedReqElSet.size();
            preparedReqElSet.erase(pRequestEl);
            //            delete pRequestEl;

            assert(preparedReqElSet.size() == (pReqElSize - 1));
            assert(preparedReqElSet.find(pRequestEl) == preparedReqElSet.end());

            LOCK_COMMIT_REQUESTS();

                assert(commitRequests.find(req) == commitRequests.end());
                int setSize = commitRequests.size();
                commitRequests.insert(req);
                assert(commitRequests.size() == (setSize + 1));

            UNLOCK_COMMIT_REQUESTS();

#ifdef PROFILE_STATS
            req->metadata.startTimeCommitedSet = Utils::GetCurrentClockTime();
#endif

        UNLOCK_PREPARED_REQUESTS();
    }


    void CoordinatorTx::updateClock(PhysicalTimeSpec time) {
        updateClockOnPrepare(time);
    }


    void CoordinatorTx::updateClockOnCommit(PhysicalTimeSpec time) {


        int64_t c_p, max;
        PhysicalTimeSpec maxTime;
        TO_64(maxTime, maxTime);

        LOCK_HLC();
            PhysicalTimeSpec currentTime = Utils::getCurrentClockTimeIn64bitFormat();

            SET_HYB_PHYS(c_p, currentTime.Seconds);
            SET_HYB_LOG(c_p, 0);

            max = MAX(c_p, HybridClock.Seconds);
            max = MAX(max, time.Seconds);

            maxTime.Seconds = max;

            HybridClock = maxTime;

        UNLOCK_HLC();
    }


    PhysicalTimeSpec CoordinatorTx::updateClockOnPrepare(PhysicalTimeSpec time) {
        PhysicalTimeSpec updatedTime;


        int64_t c_p, max_p, max_l, hc_p, hc_l, t_p, t_l;
        PhysicalTimeSpec oldHybridClock;
        int maxCase = 0;

        LOCK_HLC();
            //This is real time and it needs to be converted to the hybrid clock format
            PhysicalTimeSpec currentTime = Utils::getCurrentClockTimeIn64bitFormat();
            SET_HYB_PHYS(c_p, currentTime.Seconds);
            SET_HYB_LOG(c_p, 0);

            /* Take the physical part from time  */
            HYB_TO_PHYS(t_p, time.Seconds);
            HYB_TO_LOG(t_l, time.Seconds);

            /* Take the physical part from the node's Hybrid clock */
            oldHybridClock = HybridClock;
            HYB_TO_PHYS(hc_p, HybridClock.Seconds);
            HYB_TO_LOG(hc_l, HybridClock.Seconds);

            //We can compare the whole time, not only the physical part

            if (c_p > hc_p) {
                if (c_p > t_p) {
                    // the current physical clock is max
                    max_p = c_p;
                    max_l = 0;
                } else {
                    //the update time is max
                    max_p = t_p;
                    max_l = t_l + 1;
                }

            } else {
                if (hc_p > t_p) {
                    //the physical part of the hybrid clock is the max
                    max_p = hc_p;
                    max_l = hc_l + 1;
                } else if (hc_p == t_p) {
                    max_p = hc_p;
                    max_l = MAX(hc_l, t_l) + 1;
                } else {
                    max_p = t_p;
                    max_l = t_l + 1;
                }
            }

            assert(max_l < MAX_LOG);
            PhysicalTimeSpec maxTime;
            TO_64(maxTime, maxTime);

            SET_HYB_PHYS(maxTime.Seconds, max_p);
            SET_HYB_LOG(maxTime.Seconds, max_l);
            HybridClock = maxTime;

            if (prevHybridClockValue >= HybridClock) {
                SLOG((boost::format(
                        "UpdateClockOnPrepare :: maxCase =%d \n prevHybridClockValue = %s  HybridClock = %s .\n time = %s HCL = %s now = %s")
                      % maxCase
                      % Utils::physicaltime2str(prevHybridClockValue)
                      % Utils::physicaltime2str(HybridClock)
                      % Utils::physicaltime2str(time)
                      % Utils::physicaltime2str(oldHybridClock)
                      % Utils::physicaltime2str(currentTime)).str());
            }

            assert(prevHybridClockValue < HybridClock);
            prevHybridClockValue = HybridClock;
            updatedTime = HybridClock;
        UNLOCK_HLC();

        return updatedTime;
    }


////////////////////////////////////////////////// partitions  //////////////////////////////////////////////////



    void CoordinatorTx::HandleInternalCommitRequest(unsigned int txId, PhysicalTimeSpec ct) {
#ifdef MEASURE_STATISTICS
        SysStats::NumInternalCommitRequests += 1;
#endif
        LocalCommitRequest(txId, ct);
    }


    PhysicalTimeSpec CoordinatorTx::LocalPrepareRequest(int txId, TxContex &cdata, const std::vector<std::string> &keys,
                                                        std::vector<std::string> &values) {

        PhysicalTimeSpec prepareTime;
        PhysicalTimeSpec before_action, after_action;
        double action_duration = 0;
        LOCK_PREPARED_REQUESTS();

            before_action = Utils::GetCurrentClockTime();

#ifndef EVENTUAL_CONSISTENCY
            prepareTime = updateClockOnPrepare(cdata.HT);

            MVKVTXStore::Instance()->updateUSTifSmaller(cdata.UST);
#else
            PhysicalTimeSpec currentTime = Utils::getCurrentClockTimeIn64bitFormat();
            int64_t tmp;
            SET_HYB_PHYS(tmp, currentTime.Seconds + 1);
            SET_HYB_LOG(tmp, 0);
            PhysicalTimeSpec nodeClock;
            nodeClock.Seconds = tmp;

            prepareTime = MAX(cdata.HT, nodeClock);
#endif //EVENTUAL_CONSISTENCY

            after_action = Utils::GetCurrentClockTime();

            action_duration = (after_action - before_action).toMilliSeconds();
            SysStats::PrepareActionDuration = SysStats::PrepareActionDuration + action_duration;
            SysStats::NumTimesPrepareActionIsExecuted++;

            PrepareRequest *pRequest = new PrepareRequest(txId, cdata, keys, values);
            PrepareRequestElement *pRequestEl = new PrepareRequestElement(txId, prepareTime);

            pRequest->prepareTime = prepareTime;

            pRequest->metadata.startTimePreparedSet = Utils::GetCurrentClockTime();

            txIdToPreparedRequestsMap[txId] = pRequest;
            assert(txIdToPreparedRequestsMap.find(txId) != txIdToPreparedRequestsMap.end());

            assert(preparedReqElSet.find(pRequestEl) == preparedReqElSet.end());
            int pReqElSetSize = preparedReqElSet.size();
            preparedReqElSet.insert(pRequestEl);
            assert(preparedReqElSet.size() == (pReqElSetSize + 1));

        UNLOCK_PREPARED_REQUESTS();

        return prepareTime;
    }

    void CoordinatorTx::InternalPrepareRequest(int txId, TxContex &cdata, const std::vector<std::string> &keys,
                                               std::vector<std::string> &values, int srcPartitionId,
                                               int srcDCId) {
#ifdef MEASURE_STATISTICS
        SysStats::NumInternalPrepareRequests += 1;
#endif


        PhysicalTimeSpec prepareTime = LocalPrepareRequest(txId, cdata, keys, values);

        PbRpcPrepareRequestResult opResult;

        opResult.set_id(txId);
        opResult.set_srcpartition(_partitionId);
        opResult.set_srcdatacenter(_replicaId);
        opResult.mutable_pt()->set_seconds(prepareTime.Seconds);
        opResult.mutable_pt()->set_nanoseconds(prepareTime.NanoSeconds);

#ifdef MEASURE_STATISTICS
        opResult.set_blockduration(cdata.timeWaitedOnPrepareXact);
#endif

        if (srcDCId == _replicaId) //The request originates from a partition in the local data center
        {
            assert(std::find(_myLocalPartitionsIDs.begin(), _myLocalPartitionsIDs.end(), srcPartitionId) !=
                   _myLocalPartitionsIDs.end());

            _partitionWritesClients[srcPartitionId]->SendPrepareReply(opResult);

        } else // the request is from a remote DC that is using this partition as a remote one
        {
            assert(std::find(_currentPartition.adopteeNodesIndexes.begin(),
                             _currentPartition.adopteeNodesIndexes.end(),
                             std::make_pair(srcPartitionId, srcDCId)) !=
                   _currentPartition.adopteeNodesIndexes.end());

            _adopteePartitionWritesClients[srcPartitionId][srcDCId]->SendPrepareReply(opResult);
        }
    }

    void CoordinatorTx::HandleInternalPrepareReply(unsigned int id, int srcPartition, PhysicalTimeSpec pt,
                                                   double blockDuration) {
        Transaction *tx = GetTransaction(id);
        {
            //  std::lock_guard<std::mutex> lk(prepareTimeMutex);
            tx->partToPrepReqMap[srcPartition]->prepareTime = pt;
            tx->partToPrepReqMap[srcPartition]->prepareBlockDuration = blockDuration;
        }
        tx->prepareReqWaitHandle->SetIfCountZero();
    }


////////////////////////////////////////////////// update replication //////////////////////////////////////////////////

    bool CoordinatorTx::HandlePropagatedUpdate(std::vector<PropagatedUpdate *> &updates) {
#ifdef MEASURE_STATISTICS
        if (!updates.empty()) {
            SysStats::NumRecvUpdateReplicationMsgs += 1;
            SysStats::NumRecvUpdateReplications += updates.size();
        }
#endif

        return MVKVTXStore::Instance()->HandlePropagatedUpdate(updates);
    }

    bool CoordinatorTx::HandleHeartbeat(Heartbeat &hb, int srcReplica) {
#ifdef MEASURE_STATISTICS
        SysStats::NumRecvHeartbeats += 1;
#endif
//        SLOG((boost::format("MVKVTXStore for partition %s: HandleHeartbeat from source replica %d.") % _currentPartition.toString() % srcReplica).str());
        return MVKVTXStore::Instance()->HandleHeartbeat(hb, srcReplica);
    }

    PhysicalTimeSpec CoordinatorTx::calculateUpdateBatchTime() {

        PhysicalTimeSpec ub;
        LOCK_PREPARED_REQUESTS();
            TO_64(ub, ub);
            /* Case where there are PrepareRequests waiting */
            if (preparedReqElSet.size() > 0) {
                std::set<PrepareRequestElement *, preparedRequestsElementComparator>::iterator it;
                // Elements of preparedReqElSet are ordered by their prepare time,
                // so the first element of the set would be the element with lowest propose time

                ub = (*(preparedReqElSet.begin()))->prepareTime;
                ub.Seconds = ub.Seconds - 1;

#ifndef EVENTUAL_CONSISTENCY
                if (ub < ubPrevValue) {
                    SLOG((boost::format(
                            "calculateUpdateBatchTime CASE 1 :: UB = %s UBPrevValue = %s from case %d\n")
                          % Utils::physicaltime2str(ub) % Utils::physicaltime2str(ubPrevValue) %
                          prevUBCase).str());
                }
                assert(ub >= ubPrevValue);
#endif //EVENTUAL_CONSISTENCY

                ubPrevValue = ub;
                prevUBCase = 1;

            } else {

                assert(preparedReqElSet.size() == 0);
                /* Case where there are no PrepareRequests waiting */
                PhysicalTimeSpec now = Utils::getCurrentClockTimeIn64bitFormat();
                SET_HYB_PHYS(now.Seconds, now.Seconds);
                SET_HYB_LOG(now.Seconds, 0);

                ub = MAX(HybridClock, now);
                ub.Seconds = ub.Seconds - 1;

#ifndef EVENTUAL_CONSISTENCY
                if (ub < ubPrevValue) {
                    SLOG((boost::format(
                            "calculateUpdateBatchTime CASE 2 :: UB = %s UBPrevValue = %s from case %d \n")
                          % Utils::physicaltime2str(ub) % Utils::physicaltime2str(ubPrevValue) %
                          prevUBCase).str());
                }
                assert(ub >= ubPrevValue);
#endif //EVENTUAL_CONSISTENCY

                ubPrevValue = ub;
                prevUBCase = 2;
            }

        UNLOCK_PREPARED_REQUESTS();
        return ub;
    }

    void CoordinatorTx::PersistAndPropagateUpdates() {
        /*
         * Initialize replication
         */

        for (unsigned int i = 0; i < _myReplicas.size(); i++) {
            int rId = _myReplicas[i].ReplicaId;
            _replicationClients[rId]->InitializeReplication(_currentPartition);
        }

        SLOG("[INFO]: Update propagation thread is up.");

        int maxWaitTime = SysConfig::ReplicationHeartbeatInterval; // microseconds

        std::vector<LocalUpdate *> updatesToBeReplicatedQueue;
        std::set<PrepareRequest *, commitRequestsComparator>::iterator itLow, itUpp, it, it_temp;
        std::vector<PrepareRequest *> _localTxRequests;
        std::vector<PrepareRequest *>::iterator it_lc;
        bool sendHearbeat = true;
        int numUpdates = 0;
        PhysicalTimeSpec temp;

        // wait until all replicas are ready
        while (!_readyToServeRequests) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        while (true) {
            std::this_thread::sleep_for(std::chrono::microseconds(maxWaitTime));

            _localTxRequests.clear();
            PhysicalTimeSpec prevCommitReqTS;

            PhysicalTimeSpec before_action, after_action;
            double action_duration = 0;

            before_action = Utils::GetCurrentClockTime();
            PhysicalTimeSpec updateBatchTime = calculateUpdateBatchTime();
            after_action = Utils::GetCurrentClockTime();

            action_duration = (after_action - before_action).toMilliSeconds();
            SysStats::CalcUBTActionDuration = SysStats::PrepareActionDuration + action_duration;
            SysStats::NumTimesCalcUBTIsExecuted++;

            sendHearbeat = true;

            LOCK_COMMIT_REQUESTS();

                int sizeBefore = 0, sizeAfter = 0;
                sizeBefore = commitRequests.size();
                PhysicalTimeSpec prevCommT;
                if (commitRequests.size() > 0) {
                    assert(commitRequests.size() > 0);
                    /* Don't send heartbeat, there are commit requests waiting */
                    sendHearbeat = false;
                    /*
                     * Find the requests that have commit time lower than the update batch time and
                     * set them apart as local updates that need to be persisted and propagated
                     * */

                    itLow = commitRequests.begin();

                    PrepareRequest *ubPrepReq = new PrepareRequest(updateBatchTime);

                    itUpp = commitRequests.lower_bound(ubPrepReq);
                    if (itLow != itUpp) {
                        std::copy(itLow, itUpp, std::back_inserter(_localTxRequests));
                        commitRequests.erase(itLow, itUpp);
                    }

                    sizeAfter = commitRequests.size();
                    assert(sizeAfter <= sizeBefore);

                }
            UNLOCK_COMMIT_REQUESTS();

            if (sendHearbeat) {

                /* No commit requests present. Sending Heartbeats messages. */
                MVKVTXStore::Instance()->updateAndGetVV(_replicaId, updateBatchTime);

                Heartbeat hb;
                hb.PhysicalTime = updateBatchTime;

                assert(hb.PhysicalTime >= hbPhysTimePrevValue);
                hbPhysTimePrevValue = hb.PhysicalTime;
                hb.LogicalTime = MVKVTXStore::Instance()->getLocalClock();

                // send heartbeat
                for (unsigned int j = 0; j < _myReplicas.size(); j++) {
                    int rId = _myReplicas[j].ReplicaId;
#ifdef COMMENTS
                    //                SLOG((boost::format("[INFO] SENDING Heartbeat hb. physicalTime = %s .\n")
                    //                      % Utils::physicaltime2str(hb.PhysicalTime)).str());
#endif

                    _replicationClients[rId]->SendHeartbeat(hb);
                }
            } else {
                /* There are transactions that need to be persisted and replicated  */

                if (_localTxRequests.size() > 0) {
                    //Persist and send the updates

                    for (int i = 0; i < _localTxRequests.size(); i++) {

                        assert(temp <= _localTxRequests[i]->commitTime);
                        temp = _localTxRequests[i]->commitTime;
                        //                        MVKVTXStore::Instance()->updateVV(_replicaId, _localTxRequests[i]->commitTime);

                        updatesToBeReplicatedQueue.clear();
                        for (int j = 0; j < _localTxRequests[i]->keys.size(); j++) {

                            MVKVTXStore::Instance()->Write(_localTxRequests[i]->commitTime,
                                                           _localTxRequests[i]->metadata,
                                                           _localTxRequests[i]->keys[j],
                                                           _localTxRequests[i]->values[j],
                                                           updatesToBeReplicatedQueue);

#ifdef PROFILE_STATS
                            _localTxRequests[i]->metadata.endTimeCommitedSet = Utils::GetCurrentClockTime();
                            double duration = (_localTxRequests[i]->metadata.endTimeCommitedSet -
                                               _localTxRequests[i]->metadata.startTimeCommitedSet).toMilliSeconds();

                            {
                                std::lock_guard<std::mutex> lk(SysStats::timeInCommitedSet.valuesMutex);
                                SysStats::timeInCommitedSet.values.push_back(duration);
                            }
#endif

                        }

                        MVKVTXStore::Instance()->updateVV(_replicaId, updateBatchTime);

                        for (unsigned int j = 0; j < _myReplicas.size(); j++) {
                            int rId = _myReplicas[j].ReplicaId;
                            _replicationClients[rId]->SendUpdate(updatesToBeReplicatedQueue);

#ifdef MEASURE_STATISTICS
                            SysStats::NumSentUpdates += updatesToBeReplicatedQueue.size();
                            SysStats::NumSentBatches += 1;
#endif
                        }

                        for (unsigned int i = 0; i < updatesToBeReplicatedQueue.size(); i++) {
                            delete updatesToBeReplicatedQueue[i];
                        }

                        updatesToBeReplicatedQueue.clear();

                    }
                }
            }
        }
    }

///////////////// GST Tree //////////////////////////////////////////

    void CoordinatorTx::BuildPartialReplicationGSTTree() {
        std::queue<TreeNode *> treeNodes;

        // root node
        std::vector<int> allLocalDCPartitions = _myLocalPartitionsIDs;
        allLocalDCPartitions.push_back(_currentPartition.PartitionId);
        std::sort(allLocalDCPartitions.begin(), allLocalDCPartitions.end());

        int i = 0;
        int pid = allLocalDCPartitions[i];
        _treeRoot = new TreeNode();

        _treeRoot->Name = _allPartitions[pid][_replicaId].Name;
        _treeRoot->PartitionId = pid;
        _treeRoot->Parent = nullptr;
        _treeRoot->PartitionClient = (pid != _partitionId ? _partitionClients[pid] : nullptr);

        treeNodes.push(_treeRoot);

        if (pid == _partitionId) {
            _currentTreeNode = _treeRoot;
        }
        pid = allLocalDCPartitions[++i];

        // internal & leaf nodes
        while (!treeNodes.empty()) {
            TreeNode *parent = treeNodes.front();
            treeNodes.pop();

            for (int j = 0; j < SysConfig::NumChildrenOfTreeNode && i < allLocalDCPartitions.size(); ++j) {
                TreeNode *node = new TreeNode();
                node->Name = _allPartitions[pid][_replicaId].Name;
                node->PartitionId = pid;
                node->Parent = parent;
                node->PartitionClient = (pid != _partitionId ? _partitionClients[pid] : nullptr);

                treeNodes.push(node);

                if (pid == _partitionId) {
                    _currentTreeNode = node;
                }
                pid = allLocalDCPartitions[++i];

                parent->Children.push_back(node);
            }
        }

        // set current node type
        std::string nodeTypeStr;
        if (_currentTreeNode->Parent == nullptr) {
            _currentTreeNode->NodeType = TreeNodeType::RootNode;
            nodeTypeStr = "root";
        } else if (_currentTreeNode->Children.empty()) {
            _currentTreeNode->NodeType = TreeNodeType::LeafNode;
            nodeTypeStr = "leaf";
        } else {
            _currentTreeNode->NodeType = TreeNodeType::InternalNode;
            nodeTypeStr = "internal";
        }

        _gstRoundNum = 0;

        std::string parentStr = _currentTreeNode->Parent ?
                                std::to_string(_currentTreeNode->Parent->PartitionId) : "null";
        std::string childrenStr;
        for (unsigned int i = 0; i < _currentTreeNode->Children.size(); ++i) {
            childrenStr += std::to_string(_currentTreeNode->Children[i]->PartitionId) + " ";
        }
        SLOG("I'm a/an " + nodeTypeStr + " node. Parent is " + parentStr + ". Children are " +
             childrenStr);
    }


    void CoordinatorTx::BuildGSTTree() {
        std::queue<TreeNode *> treeNodes;

        // root node
        int i = 0;
        _treeRoot = new TreeNode();
        _treeRoot->Name = _allPartitions[i][_replicaId].Name;
        _treeRoot->PartitionId = i;
        _treeRoot->Parent = nullptr;
        _treeRoot->PartitionClient = (i != _partitionId ? _partitionClients[i] : nullptr);

        treeNodes.push(_treeRoot);

        if (i == _partitionId) {
            _currentTreeNode = _treeRoot;
        }
        i += 1;

        // internal & leaf nodes
        while (!treeNodes.empty()) {
            TreeNode *parent = treeNodes.front();
            treeNodes.pop();

            for (int j = 0; j < SysConfig::NumChildrenOfTreeNode && i < _numPartitions; ++j) {
                TreeNode *node = new TreeNode();
                node->Name = _allPartitions[i][_replicaId].Name;
                node->PartitionId = i;
                node->Parent = parent;
                node->PartitionClient = (i != _partitionId ? _partitionClients[i] : nullptr);

                treeNodes.push(node);

                if (i == _partitionId) {
                    _currentTreeNode = node;
                }
                ++i;

                parent->Children.push_back(node);
            }
        }

        // set current node type
        std::string nodeTypeStr;
        if (_currentTreeNode->Parent == nullptr) {
            _currentTreeNode->NodeType = TreeNodeType::RootNode;
            nodeTypeStr = "root";
        } else if (_currentTreeNode->Children.empty()) {
            _currentTreeNode->NodeType = TreeNodeType::LeafNode;
            nodeTypeStr = "leaf";
        } else {
            _currentTreeNode->NodeType = TreeNodeType::InternalNode;
            nodeTypeStr = "internal";
        }

        _gstRoundNum = 0;

        std::string parentStr = _currentTreeNode->Parent ?
                                std::to_string(_currentTreeNode->Parent->PartitionId) : "null";
        std::string childrenStr;
        for (unsigned int i = 0; i < _currentTreeNode->Children.size(); ++i) {
            childrenStr += std::to_string(_currentTreeNode->Children[i]->PartitionId) + " ";
        }
        SLOG("I'm a/an " + nodeTypeStr + " node. Parent is " + parentStr + ". Children are " +
             childrenStr);
    }


    void CoordinatorTx::HandleGSTFromParent(PhysicalTimeSpec parentGST) {

        MVKVTXStore::Instance()->updateGSTifSmaller(parentGST);

        if (_currentTreeNode->NodeType == TreeNodeType::InternalNode) {
            // send GST to children
            for (unsigned int i = 0; i < _currentTreeNode->Children.size(); ++i) {
                _currentTreeNode->Children[i]->PartitionClient->SendGST(parentGST);
            }
        } else if (_currentTreeNode->NodeType == TreeNodeType::LeafNode) {
            // start another round of GST computation
            _gstRoundNum += 1;

#ifdef MEASURE_STATISTICS
            SysStats::NumGSTRounds += 1;
#endif

            PhysicalTimeSpec gst;

            MVKVTXStore::Instance()->getCurrentGlobalValue(gst);

            _currentTreeNode->Parent->PartitionClient->SendLST(gst, _gstRoundNum);
        }
    }


    void CoordinatorTx::HandleGSTAndUSTFromParent(PhysicalTimeSpec parent_gst,
                                                  PhysicalTimeSpec ust) {

        MVKVTXStore::Instance()->updateGSTifSmaller(parent_gst);
        MVKVTXStore::Instance()->updateUSTifSmaller(ust);

        if (_currentTreeNode->NodeType == TreeNodeType::InternalNode) {
            // send GST and UST to children
            for (unsigned int i = 0; i < _currentTreeNode->Children.size(); ++i) {
                _currentTreeNode->Children[i]->PartitionClient->SendGSTAndUST(parent_gst, ust);
            }
        } else if (_currentTreeNode->NodeType == TreeNodeType::LeafNode) {
            // start another round of GST computation
            _gstRoundNum += 1;

#ifdef MEASURE_STATISTICS
            SysStats::NumGSTRounds += 1;
#endif

            PhysicalTimeSpec gst;

            MVKVTXStore::Instance()->getCurrentGlobalValue(gst);

            _currentTreeNode->Parent->PartitionClient->SendLST(gst, _gstRoundNum);
        }
    }

    void CoordinatorTx::HandleLSTFromChildren(PhysicalTimeSpec lst, int round) {

        assert(_currentTreeNode->NodeType != TreeNodeType::LeafNode);

        std::lock_guard<std::mutex> lk(_minLSTMutex);

        if (_receivedLSTCounts.find(round) == _receivedLSTCounts.end()) {
            _receivedLSTCounts[round] = 1;
            _minLSTs[round] = lst;
        } else {
            _receivedLSTCounts[round] += 1;
            _minLSTs[round] = MIN(_minLSTs[round], lst);
        }

        while (true) {
            int currentRound = _gstRoundNum + 1;

            if (_receivedLSTCounts.find(currentRound) != _receivedLSTCounts.end()) {
                unsigned int lstCount = _receivedLSTCounts[currentRound];
                PhysicalTimeSpec minGST = _minLSTs[currentRound];

                if (lstCount == _currentTreeNode->Children.size()) {

                    PhysicalTimeSpec dcGST;
                    MVKVTXStore::Instance()->getCurrentGlobalValue(dcGST);

                    minGST = MIN(minGST, dcGST);

                    // received all LSTs from children
                    if (_currentTreeNode->NodeType == TreeNodeType::InternalNode) {

                        // send LST to parent
                        _currentTreeNode->Parent->PartitionClient->SendLST(minGST, currentRound);

                    } else if (_currentTreeNode->NodeType == TreeNodeType::RootNode) {
                        // update GST at root

                        // set the local gst entry with the DC GST
                        MVKVTXStore::Instance()->updateGSTifSmaller(minGST);

                        assert(minGST <= MVKVTXStore::Instance()->GetVV(_replicaId));

                        pushGlobalStabilizationTimeToDCPeers(minGST);
                    }

                    _receivedLSTCounts.erase(currentRound);
                    _minLSTs.erase(currentRound);
                    _gstRoundNum = currentRound;

                } else {
                    break;
                }
            } else {

                break;
            }
        }

    }

    PhysicalTimeSpec CoordinatorTx::getGST() {

        return MVKVTXStore::Instance()->GetGSTforDCId(_replicaId);
    };


    PhysicalTimeSpec CoordinatorTx::getUST() {

        return MVKVTXStore::Instance()->getUST();
    };

    std::vector<int> CoordinatorTx::getRemotePartitionsIds() {
        return _myRemotePartitionsIDs;
    }

} // namespace scc

