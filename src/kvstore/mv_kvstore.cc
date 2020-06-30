#include "kvstore/mv_kvstore.h"
#include "mv_kvstore.h"


namespace scc {

    MVKVTXStore *MVKVTXStore::Instance() {
        static MVKVTXStore store;
        return &store;
    }

    MVKVTXStore::MVKVTXStore() {
        for (int i = 0; i < SysConfig::NumItemIndexTables; i++) {
            _indexTables.push_back(new ItemIndexTable);
            _indexTableMutexes.push_back(new std::mutex);
        }
    }

    MVKVTXStore::~MVKVTXStore() {
        for (int i = 0; i < SysConfig::NumItemIndexTables; i++) {
            delete _indexTables[i];
            delete _indexTableMutexes[i];
        }
    }

    void MVKVTXStore::SetPartitionInfo(DBPartition p, int numPartitions, int numPartitionsInLocalDC,
                                       int numReplicasPerPartition, int numDCs,
                                       std::unordered_map<int, int> &ridToIndex, std::vector<int> &localPartIDs) {
        _partitionId = p.PartitionId;
        _replicaId = p.ReplicaId;
        _numPartitions = numPartitions;
        _numPartitionsInLocalDC = numPartitionsInLocalDC;
        _numReplicasPerPartition = numReplicasPerPartition;
        _ridToVVIndex = ridToIndex;
        _numDCs = numDCs;

        _pidToGSVIndex[_partitionId] = 0; // the local partition will always be on position 0 in the stabilization vector
        for (int i = 0; i < localPartIDs.size(); i++) {
            _pidToGSVIndex[localPartIDs[i]] = i + 1;
        }

#ifdef CLUSTER_CONFIG_PRINTS
        SLOG("===============================================================================");
        string map_str = Utils::print_map(_ridToVVIndex);
        SLOG((boost::format("[DEBUG]: _ridToVVIndex IS  => \n%s") % map_str).str());
        SLOG("===============================================================================");
        SLOG((boost::format("[DEBUG]: REPLICATION FACTOR(again) IS :  %d ") % _numReplicasPerPartition).str());
        SLOG("===============================================================================");
        map_str = Utils::print_map(_pidToGSVIndex);
        SLOG((boost::format("[DEBUG]: _pidToGSVIndex IS  => \n%s") % map_str).str());
        SLOG("===============================================================================");
#endif

    }

    void MVKVTXStore::Initialize() {

        _localClock = 0;
        _replicaClock = 0;

        _pendingInvisibleVersions.resize(_numReplicasPerPartition);
        _numCheckedVersions = 0;

        PhysicalTimeSpec pt(0, 0);
        for (int i = 0; i < _numReplicasPerPartition; ++i) {
            _LVV.push_back(0);

            _HVV.push_back(pt);
            _HVV_latest.push_back(pt);

        }


        _initItemVersionUT = pt;

#ifdef SPIN_LOCK
        _gst_lock = 0;
        _pvv_lock = 0;
#endif

        for (int i = 0; i < 32; i++) {
            _hvv_l[i] = 0;
        }


        for (int i = 0; i < _numDCs; i++) {
            _GSTs.push_back(pt);
            _GSTsMutexes.push_back(new std::mutex);
        }

        _UST = pt;

        SLOG("[INFO]: MVKVTXStore initialized!");
    }

    void MVKVTXStore::GetSysClockTime(PhysicalTimeSpec &physicalTime, int64_t &logicalTime) {


        int64_t l_p, c_p;
        LOCK_VV(_replicaId);
            logicalTime = _localClock;
            physicalTime = Utils::GetCurrentClockTime();
            TO_64(physicalTime, physicalTime);
            SET_HYB_PHYS(l_p, physicalTime.Seconds);
            SET_HYB_LOG(l_p, 0);
            //Update only if local real phyisical catches up with hybrid
            //TODO: should we still move it forward by Delta?
            if (l_p > _VV(_replicaId).Seconds) {
                _VV(_replicaId).Seconds = l_p;
            }
            physicalTime = _VV(_replicaId);
        UNLOCK_VV(_replicaId);

    }

    // only used for initialization (key loading)

    bool MVKVTXStore::Add(const std::string &key, const std::string &value) {


        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        // check if key exists
        bool keyExisting = false;
        {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[ti]));
            if (_indexTables[ti]->find(key) != _indexTables[ti]->end()) {
                keyExisting = true;
            }

        }

        if (!keyExisting) {
            // create new item anchor and version
            ItemAnchor *newAnchor = new ItemAnchor(key, _numReplicasPerPartition, _replicaId, _ridToVVIndex);
            ItemVersion *newVersion = new ItemVersion(value);


            newVersion->LUT = 0;
            newVersion->RIT = Utils::GetCurrentClockTime();
#ifdef MEASURE_VISIBILITY_LATENCY
            newVersion->CreationTime = _initItemVersionUT;
#endif
            newVersion->UT = _initItemVersionUT;
            newVersion->SrcReplica = 0;
            newVersion->DST = _initItemVersionUT;

#ifdef LOCALITY
            int keyInPartition = std::stoi(key) / _numPartitions;
            int srcReplica = _replicaId;

            newVersion->SrcReplica = srcReplica;
#endif


            newVersion->SrcPartition = _partitionId;
            newVersion->Persisted = true;


            newAnchor->InsertVersion(newVersion);

            // insert into the index if key still does not exist
            {
                std::lock_guard<std::mutex> lk(*(_indexTableMutexes[ti]));
                if (_indexTables[ti]->find(key) != _indexTables[ti]->end()) {
                    keyExisting = true;

                    // key already exists, release pre-created objects
                    delete newAnchor;
                    delete newVersion;

                } else {
                    _indexTables[ti]->insert(ItemIndexTable::value_type(key, newAnchor));
                }
            }
        }

        return !keyExisting;
    }

    bool MVKVTXStore::AddAgain(const std::string &key, const std::string &value) {

        assert(false); //Not sure it works with other  flags now
        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        //Treat key as non-existing



        //Retrieve existing anchor
        ItemAnchor *newAnchor = _indexTables[ti]->at(key);
        ItemVersion *newVersion = new ItemVersion(value);

        newVersion->LUT = 0;
        newVersion->RIT = Utils::GetCurrentClockTime();
#ifdef MEASURE_VISIBILITY_LATENCY
        newVersion->CreationTime = _initItemVersionUT;
#endif
        PhysicalTimeSpec putTime = _initItemVersionUT;
        putTime.Seconds += 1;//Newer version w.r.t. previous one
        TO_64(newVersion->UT, putTime);
        newVersion->SrcReplica = 0;

#ifdef LOCALITY
        int keyInPartition = std::stoi(key) / _numPartitions;
        int srcReplica = ((double) keyInPartition) / 333333.0;
        fprintf(stdout, "(Be sure #keys/dc is correct) Adding again key %d, the %d for partition %d. Assigned to replica %d with PUT %f\n", std::stoi(key), keyInPartition,
                _partitionId, srcReplica,newVersion->UT.toMilliSeconds());
        newVersion->SrcReplica = srcReplica;
#endif

        newVersion->SrcPartition = _partitionId;
        newVersion->Persisted = true;


        newAnchor->InsertVersion(newVersion);


        // no logging for this operation

        return true;
    }


    bool
    MVKVTXStore::Write(
            PhysicalTimeSpec commitTime,

            TxContex &cdata, const std::string &key, const std::string &value,
            std::vector<LocalUpdate *> &updatesToBeReplicatedQueue) {
#ifndef SIXTY_FOUR_BIT_CLOCK
        assert(false);
#endif

        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        ItemAnchor *itemAnchor = NULL;

        /**TODO: return the lock */
        {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[ti]));
            try {
                itemAnchor = _indexTables[ti]->at(key);
            } catch (std::out_of_range &e) {
                if (SysConfig::Debugging) {
                    SLOG((boost::format("MVKVTXStore: Set key %s does not exist.") % key).str());
                }
                assert(false);
                return false;
            }
        }

        ItemVersion *newVersion = new ItemVersion(value);

        // deleted at Coordinator::SendUpdate()
        LocalUpdate *update = new LocalUpdate();
        update->UpdatedItemAnchor = itemAnchor;
        update->UpdatedItemVersion = newVersion;

        newVersion->SrcPartition = _partitionId;
        newVersion->SrcReplica = _replicaId;
        newVersion->Persisted = false;
        newVersion->Key = key;

        newVersion->DST = cdata.UST;

        LOCK_VV(_replicaId);

            _localClock += 1;
            _replicaClock += 1;
            _LVV[_ridToVVIndex[_replicaId]] = _localClock;

            newVersion->LUT = _localClock;
            PhysicalTimeSpec now = Utils::GetCurrentClockTime(), currentTime; //real time, used for stats
            newVersion->RIT = now; //real time
            TO_64(currentTime, now);
#ifdef MEASURE_VISIBILITY_LATENCY
            newVersion->CreationTime = currentTime; //real time in64_bit format
#endif
            newVersion->UT = commitTime;


            itemAnchor->InsertVersion(newVersion);

            PbLogRecord record;

            record.set_key(newVersion->Key);
            record.set_value(newVersion->Value);

            record.mutable_ut()->set_seconds(newVersion->UT.Seconds);
            record.mutable_ut()->set_nanoseconds(newVersion->UT.NanoSeconds);

            record.set_srcreplica(newVersion->SrcReplica);
            record.set_lut(newVersion->LUT);

#ifdef MEASURE_VISIBILITY_LATENCY
            record.mutable_creationtime()->set_seconds(newVersion->CreationTime.Seconds);
            record.mutable_creationtime()->set_nanoseconds(newVersion->CreationTime.NanoSeconds);
#endif

            update->SerializedRecord = record.SerializeAsString();


            updatesToBeReplicatedQueue.push_back(update);

        UNLOCK_VV(_replicaId);

#ifdef MEASURE_VISIBILITY_LATENCY
        LOCK_PENDING_INVISIBLE_QUEUE();
            _pendingInvisibleVersions[_ridToVVIndex[_replicaId]].push(newVersion);
        UNLOCK_PENDING_INVISIBLE_QUEUE();
#endif
        return true;
    }

#ifdef BLOCKING_ON

    void MVKVTXStore::WaitOnTxSlice(unsigned int txId, TxContex &cdata) {

#ifdef FAKE_TX_WAIT
        cdata.waited_xact = 0;
        return;
#endif //FAKE_TX_WAIT

#ifdef MEASURE_STATISTICS
        bool slept = false;
        PhysicalTimeSpec initWait;
#endif //MEASURE_STATISTICS


          while (cdata.UST >= getVVmin()) {

#ifdef MEASURE_STATISTICS
            if (!slept) {
                slept = true;
                initWait = Utils::GetCurrentClockTime();
            }
#endif //MEASURE_STATISTICS

            int useconds = 500;

            std::this_thread::sleep_for(std::chrono::microseconds(useconds));
        } // end of while

#ifdef MEASURE_STATISTICS
        if (slept) {
            PhysicalTimeSpec currentTime = Utils::GetCurrentClockTime();
            PhysicalTimeSpec endWait = currentTime - initWait;
            cdata.waited_xact = endWait.toMilliSeconds();

            SysStats::NumTxReadBlocks++;
            SysStats::TxReadBlockDuration = SysStats::TxReadBlockDuration + endWait.toMilliSeconds();
        }
#endif //MEASURE_STATISTICS
    }
#endif


    bool MVKVTXStore::LocalTxSliceGet(const TxContex &cdata, const std::string &key, std::string &value) {
        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        ItemAnchor *itemAnchor = NULL;
        {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[ti]));
            if (_indexTables[ti]->find(key) == _indexTables[ti]->end()) {
                assert(false);
            } else {
                itemAnchor = _indexTables[ti]->at(key);
            }
        }

        PhysicalTimeSpec startTime = Utils::GetCurrentClockTime();

#ifndef EVENTUAL_CONSISTENCY
        ItemVersion *getVersion = itemAnchor->LatestSnapshotVersion(cdata.UST);
#else
        ItemVersion *getVersion = itemAnchor->LatestVersion();
#endif // EVENTUAL_CONSISTENCY

        PhysicalTimeSpec endTime = Utils::GetCurrentClockTime();

        double duration = (endTime - startTime).toMilliSeconds();
        SysStats::ChainLatencySum = SysStats::ChainLatencySum + duration;
        SysStats::NumChainLatencyMeasurements++;

        assert(getVersion != NULL);

        value = getVersion->Value;

        return true;
    }


    bool MVKVTXStore::ShowItem(const std::string &key, std::string &itemVersions) {
        int ti = Utils::strhash(key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        ItemAnchor *itemAnchor = NULL;

        /** TODO: return the lock */
        {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[ti]));
            if (_indexTables[ti]->find(key) == _indexTables[ti]->end()) {
                return false;
            } else {
                itemAnchor = _indexTables[ti]->at(key);
            }
        }

        itemVersions = itemAnchor->ShowItemVersions();

        return true;
    }

    bool MVKVTXStore::ShowDB(std::string &allItemVersions) {
        for (int i = 0; i < SysConfig::NumItemIndexTables; i++) {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[i]));
            for (ItemIndexTable::iterator it = _indexTables[i]->begin();
                 it != _indexTables[i]->end(); it++) {
                std::string itemVersions = it->second->ShowItemVersions();
                allItemVersions += (itemVersions + "\n");
            }
        }
        return true;
    }

    bool MVKVTXStore::ShowStateTx(std::string &stateStr) {

        std::vector<PhysicalTimeSpec> vv; //hvv or pvv depending on the protocol
        std::vector<int64_t> lvv;
        PhysicalTimeSpec gst, lst;

        StalenessStatistics stat;
        VisibilityStatistics vs;

        vv = GetVV();

#ifdef MEASURE_STATISTICS
        //calculateStalenessStatistics(stat);
#endif
        // ShowVisibilityLatency(vs);


        stateStr += addToStateStr("replica clock", _replicaClock);
        stateStr += addToStateStr("GST", Utils::physicaltime2str(gst));
        stateStr += addToStateStr("LST", Utils::physicaltime2str(lst));
        stateStr += addToStateStr("VV", Utils::physicaltv2str(vv));
        stateStr += addToStateStr("LVV", Utils::logicaltv2str(lvv));

        stateStr += addToStateStr("NumPublicTxStartRequests", (long) SysStats::NumPublicTxStartRequests);
        stateStr += addToStateStr("NumPublicTxReadRequests", (long) SysStats::NumPublicTxReadRequests);
        stateStr += addToStateStr("NumPublicTxWriteRequests", (long) SysStats::NumPublicTxWriteRequests);
        stateStr += addToStateStr("NumPublicTxCommitRequests", (long) SysStats::NumPublicTxCommitRequests);

        stateStr += addToStateStr("NumInternalTxSliceReadRequests", (long) SysStats::NumInternalTxSliceReadRequests);
        stateStr += addToStateStr("NumInternalCommitRequests", (long) SysStats::NumInternalCommitRequests);
        stateStr += addToStateStr("NumInternalPrepareRequests", (long) SysStats::NumInternalPrepareRequests);

        stateStr += addToStateStr("NumTransactions", (int) SysStats::NumTransactions);
        stateStr += addToStateStr("NumTxReadBlocks", (long) SysStats::NumTxReadBlocks);
        stateStr += addToStateStr("TxReadBlockDuration", (((double) SysStats::TxReadBlockDuration)));
        stateStr += addToStateStr("NumTxReadPhases", (long) SysStats::NumTxReadPhases);
        stateStr += addToStateStr("NumCommitedTxs", (long) SysStats::NumCommitedTxs);

        stateStr += addToStateStr("NumDelayedLocalUpdates", (long) SysStats::NumDelayedLocalUpdates);

        stateStr += addToStateStr("NumPendingPropagatedUpdates", (long) SysStats::NumPendingPropagatedUpdates);
        stateStr += addToStateStr("NumReplicatedBytes", (long) LogManager::Instance()->NumReplicatedBytes);
        stateStr += addToStateStr("NumSentReplicationBytes", (long) SysStats::NumSentReplicationBytes);
        stateStr += addToStateStr("NumRecvReplicationBytes", (long) SysStats::NumRecvReplicationBytes);
        stateStr += addToStateStr("NumRecvUpdateReplicationMsgs", (long) SysStats::NumRecvUpdateReplicationMsgs);
        stateStr += addToStateStr("NumRecvUpdateReplications", (long) SysStats::NumRecvUpdateReplications);
        stateStr += addToStateStr("NumReceivedPropagatedUpdates", (int) SysStats::NumReceivedPropagatedUpdates);
        stateStr += addToStateStr("NumSentUpdates", (int) SysStats::NumSentUpdates);
        stateStr += addToStateStr("NumSentBatches", (int) SysStats::NumSentBatches);
        stateStr += addToStateStr("NumReceivedBatches", (int) SysStats::NumReceivedBatches);
        stateStr += addToStateStr("NumRecvHeartbeats", (int) SysStats::NumRecvHeartbeats);
        stateStr += addToStateStr("NumUpdatesStoredInLocalUpdateQueue",
                                  (int) SysStats::NumUpdatesStoredInLocalUpdateQueue);
        stateStr += addToStateStr("NumUpdatesStoredInToPropagateLocalUpdateQueue",
                                  (int) SysStats::NumUpdatesStoredInToPropagateLocalUpdateQueue);

        stateStr += addToStateStr("NumSentRSTs", (long) SysStats::NumSentRSTs);
        stateStr += addToStateStr("NumRecvRSTs", (long) SysStats::NumRecvRSTs);
        stateStr += addToStateStr("NumSentRSTBytes", (long) SysStats::NumSentRSTBytes);
        stateStr += addToStateStr("NumRecvRSTBytes", (long) SysStats::NumRecvRSTBytes);

        stateStr += addToStateStr("NumGSTRounds", (long) SysStats::NumGSTRounds);
        stateStr += addToStateStr("NumRecvGSTs", (long) SysStats::NumRecvGSTs);
        stateStr += addToStateStr("NumRecvGSTBytes", (long) SysStats::NumRecvGSTBytes);
        stateStr += addToStateStr("NumSentGSTBytes", (long) SysStats::NumSentGSTBytes);
        stateStr += addToStateStr("NumSentUSTBytes", (long) SysStats::NumSentUSTBytes);
        stateStr += addToStateStr("NumRecvUSTBytes", (long) SysStats::NumRecvUSTBytes);

        stateStr += addToStateStr("NumLatencyCheckedVersions", (long) _numCheckedVersions);

        stateStr += addToStateStr("NumReturnedItemVersions", (long) SysStats::NumReturnedItemVersions);
        stateStr += addToStateStr("NumReturnedStaleItemVersions", (long) SysStats::NumReturnedStaleItemVersions);
        stateStr += addToStateStr("AvgNumFresherVersionsInItemChain", stat.averageNumFresherVersionsInItemChain);
        stateStr += addToStateStr("AverageUserPerceivedStalenessTime", stat.averageUserPerceivedStalenessTime);
        stateStr += addToStateStr("MinUserPerceivedStalenessTime", stat.minStalenessTime);
        stateStr += addToStateStr("MaxUserPerceivedStalenessTime", stat.maxStalenessTime);
        stateStr += addToStateStr("MedianUserPerceivedStalenessTime", stat.medianStalenessTime);
        stateStr += addToStateStr("90PercentileStalenessTime", stat._90PercentileStalenessTime);
        stateStr += addToStateStr("95PercentileStalenessTime", stat._95PercentileStalenessTime);
        stateStr += addToStateStr("99PercentileStalenessTime", stat._99PercentileStalenessTime);


        stateStr += addToStateStr("sum_fresher_versions",
                                  (int) std::accumulate(SysStats::NumFresherVersionsInItemChain.begin(),
                                                        SysStats::NumFresherVersionsInItemChain.end(), 0));
        stateStr += addToStateStr("NumSentGSTBytes", (int) SysStats::NumSentGSTBytes);
        stateStr += addToStateStr("NumRecvUSTBytes", (int) SysStats::NumRecvUSTBytes);
        stateStr += addToStateStr("NumSentReplicationBytes", (int) SysStats::NumSentReplicationBytes);
        stateStr += addToStateStr("NumRecvReplicationBytes", (int) SysStats::NumRecvReplicationBytes);
        stateStr += addToStateStr("NumSentBatches", (int) SysStats::NumSentBatches);
        stateStr += addToStateStr("NumReceivedBatches", (int) SysStats::NumReceivedBatches);
        stateStr += addToStateStr("SumDelayedLocalUpdates", (int) SysStats::SumDelayedLocalUpdates);

        stateStr += addToStateStr("SumDelayedLocalUpdates", (int) SysStats::SumDelayedLocalUpdates);

        return true;

    }

    bool MVKVTXStore::ShowStateTxCSV(std::string &stateStr) {

        char separator = ';';

        std::vector<PhysicalTimeSpec> vv; //hvv or pvv depending on the protocol
        std::vector<int64_t> lvv;
        PhysicalTimeSpec gst, lst;
        PhysicalTimeVector gsv, rst;

        StalenessStatistics stat;
        VisibilityStatistics vs, vsLocal;
        std::string protocol = "";

#ifdef EVENTUAL_CONSISTENCY
#ifdef NO_START_TX
        protocol = "NO_START_TX_EVENTUAL_CONSISTENCY";
#else
        protocol = "EVENTUAL_CONSISTENCY";
#endif //NO_START_TX
#else

#ifndef BLOCKING_ON
        protocol = "PARIS";
#else
        protocol = "BLOCKING";
#endif

#endif

        vv = GetVV();
        lvv = GetLVV();

#ifdef PROFILE_STATS
        SysStats::timeInPreparedSet.calculate();
        SysStats::timeInCommitedSet.calculate();
#endif

#ifdef MEASURE_STATISTICS
        calculateStalenessStatistics(stat);
#endif
        ShowVisibilityLatency(vs, true); //remote => now only this mode exists and is for both local and remote
//        ShowVisibilityLatency(vsLocal, false); //local

        std::string serverName = stateStr + std::to_string(_partitionId) + "_" + std::to_string(_replicaId);
        stateStr = "";
        /* First add server statistics header */
        std::vector<std::string> header{"Protocol", "LocalClock", "ReplicaClock", "VV", "LVV",
                                        "NumTransactions",
                                        "NumPublicTxStartRequests",
                                        "NumPublicTxReadRequests",
                                        "NumPublicTxCommitRequests",
                                        "NumInternalTxSliceReadRequests",
                                        "NumInternalCommitRequests",
                                        "NumInternalPrepareRequests",
                                        "NumTxReadBlocks",
                                        "TxReadBlockDuration",
                                        "NumTxReadBlocksAtCoordinator",
                                        "TxReadBlockDurationAtCoordinator",
                                        "NumTxReadPhases",
                                        "NumTxPreparePhaseBlocks",
                                        "TxPreparePhaseBlockDuration",
                                        "NumTxPreparePhaseBlocksAtCoordinator",
                                        "TxPreparePhaseBlockDurationAtCoordinator",
                                        "NumSentReplicationBytes", "NumRecvReplicationBytes",
                                        "NumRecvUpdateReplicationMsgs", "NumRecvUpdateReplications",
                                        "NumReceivedPropagatedUpdates", "NumSentUpdates", "NumSentBatches",
                                        "NumReceivedBatches", "NumRecvHeartbeats", "#vals_in_timeInPreparedSet",
                                        "timeInPreparedSet_AVG",
                                        "timeInPreparedSet_MIN", "timeInPreparedSet_MAX",
                                        "#vals_in_timeInCommitedSet", "timeInCommitedSet_AVG",
                                        "timeInCommitedSet_MIN", "timeInCommitedSet_MAX", "NUM_BLOCKS_ON_PREPARE",
                                        "BLOCKS_DIFF", "NUM_NO_BLOCKS_ON_PREPARE", "NO_BLOCKS_DIFF",
                                        "NUM_TIMES_WAIT_ON_PREPARE_IS_ENTERED", "NUM_TIMES_GSS_IS_ZERO",
                                        "NUM_TIMES_DEP_TIME_IS_ZERO", "PREPARE_ACTION_DURATION",
                                        "NUM_TIMES_PREPARE_EXECUTED", "CALC_UBT_ACTION_DURATION",
                                        "NUM_TIMES_CALC_UBT_EXECUTED",
                                        "PosDiff_HybToPhys_TxStart",
                                        "CountPosDiff_HybToPhys_TxStart",
                                        "NegDiff_HybToPhys_TxStart",
                                        "CountNegDiff_HybToPhys_TxStart",
                                        "PosDiff_HybToPhys_TxRead",
                                        "CountPosDiff_HybToPhys_TxRead",
                                        "NegDiff_HybToPhys_TxRead",
                                        "CountNegDiff_HybToPhys_TxRead",
                                        "PosDiff_HybToVV_TxRead",
                                        "CountPosDiff_HybToVV_TxRead",
                                        "NegDiff_HybToVV_TxRead",
                                        "CountNegDiff_HybToVV_TxRead",
                                        "PosDiff_HybToVV_TxRead_Block",
                                        "CountPosDiff_HybToVV_TxRead_Block",
                                        "NegDiff_HybToVV_TxRead_Block",
                                        "CountNegDiff_HybToVV_TxRead_Block",
                                        "NumReturnedItemVersions",
                                        "NumReturnedStaleItemVersions",
                                        "AvgNumFresherVersionsInItemChain",
                                        "AverageUserPerceivedStalenessTime",
                                        "MinUserPerceivedStalenessTime",
                                        "MaxUserPerceivedStalenessTime",
                                        "MedianUserPerceivedStalenessTime",
                                        "90PercentileStalenessTime",
                                        "95PercentileStalenessTime",
                                        "99PercentileStalenessTime",
                                        "sum_fresher_versions",
                                        "NumReadLocalItems",
                                        "NumReadLocalStaleItems",
                                        "NumReadRemoteItems",
                                        "NumReadRemoteStaleItems",
                                        "NumReadItemsFromKVStore",
                                        "ServerReadLatencySum",
                                        "NumServerReadLatencyMeasurements",
                                        "ServerReadLatencyAvg",
                                        "ChainLatencySum",
                                        "NumChainLatencyMeasurements",
                                        "ChainLatencyAvg",
                                        "CohordHandleInternalSliceReadLatencySum",
                                        "NumCohordHandleInternalSliceReadLatencyMeasurements",
                                        "CohordHandleInternalSliceReadLatencyAvg",
                                        "CohordSendingReadResultsLatencySum",
                                        "NumCohordSendingReadResultsLatencyMeasurements",
                                        "CohordSendingReadResultsLatencyAvg",
                                        "CoordinatorMappingKeysToShardsLatencySum",
                                        "NumCoordinatorMappingKeysToShardsLatencyMeasurements",
                                        "CoordinatorMappingKeysToShardsLatencyAvg",
                                        "CoordinatorSendingReadReqToShardsLatencySum",
                                        "NumCoordinatorSendingReadReqToShardsLatencyMeasurements",
                                        "CoordinatorSendingReadReqToShardsLatencyAvg",
                                        "CoordinatorReadingLocalKeysLatencySum",
                                        "NumCoordinatorReadingLocalKeysLatencyMeasurements",
                                        "CoordinatorReadingLocalKeysLatencyAvg",
                                        "CoordinatorProcessingOtherShardsReadRepliesLatencySum",
                                        "NumCoordinatorProcessingOtherShardsReadRepliesLatencyMeasurements",
                                        "CoordinatorProcessingOtherShardsReadRepliesLatencyAvg",
                                        "CoordinatorLocalKeyReadsLatencySum",
                                        "NumCoordinatorLocalKeyReadsLatencyMeasurements",
                                        "CoordinatorLocalKeyReadsLatencyAvg",
                                        "CoordinatorWaitingForShardsRepliesLatencySum",
                                        "NumCoordinatorWaitingForShardsRepliesMeasurements",
                                        "CoordinatorWaitingForShardsRepliesLatencyAvg",
                                        "CoordinatorReadReplyHandlingLatencySum",
                                        "NumCoordinatorReadReplyHandlingMeasurements",
                                        "CoordinatorReadReplyHandlingLatencyAvg",
                                        "CoordinatorCommitLatencySum",
                                        "NumCoordinatorCommitMeasurements",
                                        "CoordinatorCommitLatencyAvg",
                                        "CoordinatorCommitMappingKeysToShardsLatencySum",
                                        "NumCoordinatorCommitMappingKeysToShardsLatencyMeasurements",
                                        "CoordinatorCommitMappingKeysToShardsLatencyAvg",
                                        "CoordinatorCommitSendingPrepareReqToShardsLatencySum",
                                        "NumCoordinatorCommitSendingPrepareReqToShardsLatencyMeasurements",
                                        "CoordinatorCommitSendingPrepareReqToShardsLatencyAvg",
                                        "CoordinatorCommitLocalPrepareLatencySum",
                                        "NumCoordinatorCommitLocalPrepareLatencyMeasurements",
                                        "CoordinatorCommitLocalPrepareLatencyAvg",
                                        "CoordinatorCommitWaitingOtherShardsPrepareRepliesLatencySum",
                                        "NumCoordinatorCommitWaitingOtherShardsPrepareRepliesLatencyMeasurements",
                                        "CoordinatorCommitWaitingOtherShardsPrepareRepliesLatencyAvg",
                                        "CoordinatorCommitProcessingPrepareRepliesLatencySum",
                                        "NumCoordinatorCommitProcessingPrepareRepliesLatencyMeasurements",
                                        "CoordinatorCommitProcessingPrepareRepliesLatencyAvg",
                                        "CoordinatorCommitSendingCommitReqLatencySum",
                                        "NumCoordinatorCommitSendingCommitReqLatencyMeasurements",
                                        "CoordinatorCommitSendingCommitReqLatencySum",
                                        "NumSentGSTs",
                                        "NumRecvGSTs",
                                        "NumSentGSTBytes",
                                        "NumRecvGSTBytes",
                                        "NumSentUSTs",
                                        "NumRecvUSTs",
                                        "NumSentUSTBytes",
                                        "NumRecvUSTBytes",
                                        "NumChannels"
        };

        std::string sname = ">>>;ServerName";
        Utils::appendToCSVString(stateStr, sname);
        for (int i = 0; i < header.size(); i++) {
            Utils::appendToCSVString(stateStr, header[i]);
        }
        std::string strToken;
        for (int i = 0; i < 9; i++) {
            strToken = "Remote_Overall_" + std::to_string(((i + 1) * 10));
            Utils::appendToCSVString(stateStr, strToken);
        }
        strToken = "Remote_Overall_95";
        Utils::appendToCSVString(stateStr, strToken);
        strToken = "Remote_Overall_99";
        Utils::appendToCSVString(stateStr, strToken);

        for (int i = 0; i < 9; i++) {
            strToken = "Remote_Propagation_" + std::to_string(((i + 1) * 10));
            Utils::appendToCSVString(stateStr, strToken);
        }
        strToken = "Remote_Propagation_95";
        Utils::appendToCSVString(stateStr, strToken);
        strToken = "Remote_Propagation_99";
        Utils::appendToCSVString(stateStr, strToken);

        for (int i = 0; i < 9; i++) {
            strToken = "Remote_Visibility_" + std::to_string(((i + 1) * 10));
            Utils::appendToCSVString(stateStr, strToken);
        }
        strToken = "Remote_Visibility_95";
        Utils::appendToCSVString(stateStr, strToken);
        strToken = "Remote_Visibility_99";
        Utils::appendToCSVString(stateStr, strToken);

        for (int i = 0; i < 9; i++) {
            strToken = "Local_Overall_" + std::to_string(((i + 1) * 10));
            Utils::appendToCSVString(stateStr, strToken);
        }
        strToken = "Local_Overall_95";
        Utils::appendToCSVString(stateStr, strToken);
        strToken = "Local_Overall_99";
        Utils::appendToCSVString(stateStr, strToken);

        for (int i = 0; i < 9; i++) {
            strToken = "Local_Propagation_" + std::to_string(((i + 1) * 10));
            Utils::appendToCSVString(stateStr, strToken);
        }
        strToken = "Local_Propagation_95";
        Utils::appendToCSVString(stateStr, strToken);
        strToken = "Local_Propagation_99";
        Utils::appendToCSVString(stateStr, strToken);

        for (int i = 0; i < 9; i++) {
            strToken = "Local_Visibility_" + std::to_string(((i + 1) * 10));
            Utils::appendToCSVString(stateStr, strToken);
        }
        strToken = "Local_Visibility_95";
        Utils::appendToCSVString(stateStr, strToken);
        strToken = "Local_Visibility_99";
        Utils::appendToCSVString(stateStr, strToken);


        stateStr += "\n";
        std::string dummy = ">>>";
        /* Then add server statistics  */
        Utils::appendToCSVString(stateStr, dummy);
        Utils::appendToCSVString(stateStr, serverName);
        Utils::appendToCSVString(stateStr, protocol);
        Utils::appendToCSVString(stateStr, _localClock);
        Utils::appendToCSVString(stateStr, _replicaClock);
        Utils::appendToCSVString(stateStr, Utils::physicaltv2str(vv));
        Utils::appendToCSVString(stateStr, Utils::logicaltv2str(lvv));
        Utils::appendToCSVString(stateStr, (long) SysStats::NumCommitedTxs);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumPublicTxStartRequests);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumPublicTxReadRequests);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumPublicTxCommitRequests);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumInternalTxSliceReadRequests);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumInternalCommitRequests);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumInternalPrepareRequests);

        Utils::appendToCSVString(stateStr, (long) SysStats::NumTxReadBlocks);
        Utils::appendToCSVString(stateStr, (double) SysStats::TxReadBlockDuration);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumTxReadBlocksAtCoordinator);
        Utils::appendToCSVString(stateStr, (double) SysStats::TxReadBlockDurationAtCoordinator);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumTxReadPhases);

        Utils::appendToCSVString(stateStr, (long) SysStats::NumTxPreparePhaseBlocks);
        Utils::appendToCSVString(stateStr, (double) SysStats::TxPreparePhaseBlockDuration);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumTxPreparePhaseBlocksAtCoordinator);
        Utils::appendToCSVString(stateStr, (double) SysStats::TxPreparePhaseBlockDurationAtCoordinator);

        Utils::appendToCSVString(stateStr, (long) SysStats::NumSentReplicationBytes);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumRecvReplicationBytes);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumRecvUpdateReplicationMsgs);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumRecvUpdateReplications);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumReceivedPropagatedUpdates);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumSentUpdates);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumSentBatches);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumReceivedBatches);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumRecvHeartbeats);
        Utils::appendToCSVString(stateStr, (long) SysStats::timeInPreparedSet.values.size());
        Utils::appendToCSVString(stateStr, (double) SysStats::timeInPreparedSet.average);
        Utils::appendToCSVString(stateStr, (double) SysStats::timeInPreparedSet.min);
        Utils::appendToCSVString(stateStr, (double) SysStats::timeInPreparedSet.max);
        Utils::appendToCSVString(stateStr, (long) SysStats::timeInCommitedSet.values.size());
        Utils::appendToCSVString(stateStr, (double) SysStats::timeInCommitedSet.average);
        Utils::appendToCSVString(stateStr, (double) SysStats::timeInCommitedSet.min);
        Utils::appendToCSVString(stateStr, (double) SysStats::timeInCommitedSet.max);
        Utils::appendToCSVString(stateStr, (double) SysStats::PrepareCondBlocks);
        Utils::appendToCSVString(stateStr, (double) SysStats::PrepBlockDifference);
        Utils::appendToCSVString(stateStr, (double) SysStats::PrepareCondNoBlocks);
        Utils::appendToCSVString(stateStr, (double) SysStats::PrepNoBlockDifference);
        Utils::appendToCSVString(stateStr, (double) SysStats::NumTimesWaitOnPrepareIsEntered);
        Utils::appendToCSVString(stateStr, (double) SysStats::NumGSSisZero);
        Utils::appendToCSVString(stateStr, (double) SysStats::NumDepTimeisZero);
        Utils::appendToCSVString(stateStr, (double) SysStats::PrepareActionDuration);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumTimesPrepareActionIsExecuted);

        Utils::appendToCSVString(stateStr, (double) SysStats::CalcUBTActionDuration);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumTimesCalcUBTIsExecuted);

        Utils::appendToCSVString(stateStr, (double) SysStats::PosDiff_HybToPhys_TxStart);
        Utils::appendToCSVString(stateStr, (int) SysStats::CountPosDiff_HybToPhys_TxStart);
        Utils::appendToCSVString(stateStr, (double) SysStats::NegDiff_HybToPhys_TxStart);
        Utils::appendToCSVString(stateStr, (int) SysStats::CountNegDiff_HybToPhys_TxStart);

        Utils::appendToCSVString(stateStr, (double) SysStats::PosDiff_HybToPhys_TxRead);
        Utils::appendToCSVString(stateStr, (int) SysStats::CountPosDiff_HybToPhys_TxRead);
        Utils::appendToCSVString(stateStr, (double) SysStats::NegDiff_HybToPhys_TxRead);
        Utils::appendToCSVString(stateStr, (int) SysStats::CountNegDiff_HybToPhys_TxRead);

        Utils::appendToCSVString(stateStr, (double) SysStats::PosDiff_HybToVV_TxRead);
        Utils::appendToCSVString(stateStr, (int) SysStats::CountPosDiff_HybToVV_TxRead);
        Utils::appendToCSVString(stateStr, (double) SysStats::NegDiff_HybToVV_TxRead);
        Utils::appendToCSVString(stateStr, (int) SysStats::CountNegDiff_HybToVV_TxRead);

        Utils::appendToCSVString(stateStr, (double) SysStats::PosDiff_HybToVV_TxRead_Block);
        Utils::appendToCSVString(stateStr, (int) SysStats::CountPosDiff_HybToVV_TxRead_Block);
        Utils::appendToCSVString(stateStr, (double) SysStats::NegDiff_HybToVV_TxRead_Block);
        Utils::appendToCSVString(stateStr, (int) SysStats::CountNegDiff_HybToVV_TxRead_Block);

        /* staleness stats */
        Utils::appendToCSVString(stateStr, (long) SysStats::NumReturnedItemVersions);
        Utils::appendToCSVString(stateStr, (long) SysStats::NumReturnedStaleItemVersions);
        Utils::appendToCSVString(stateStr, (double) stat.averageNumFresherVersionsInItemChain);
        Utils::appendToCSVString(stateStr, (double) stat.averageUserPerceivedStalenessTime);
        Utils::appendToCSVString(stateStr, (double) stat.minStalenessTime);
        Utils::appendToCSVString(stateStr, (double) stat.maxStalenessTime);
        Utils::appendToCSVString(stateStr, (double) stat.medianStalenessTime);
        Utils::appendToCSVString(stateStr, (double) stat._90PercentileStalenessTime);
        Utils::appendToCSVString(stateStr, (double) stat._95PercentileStalenessTime);
        Utils::appendToCSVString(stateStr, (double) stat._99PercentileStalenessTime);
        Utils::appendToCSVString(stateStr, (long) std::accumulate(SysStats::NumFresherVersionsInItemChain.begin(),
                                                                  SysStats::NumFresherVersionsInItemChain.end(), 0));

        Utils::appendToCSVString(stateStr, (int) SysStats::NumReadLocalItems);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumReadLocalStaleItems);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumReadRemoteItems);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumReadRemoteStaleItems);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumReadItemsFromKVStore);

        Utils::appendToCSVString(stateStr, (double) SysStats::ServerReadLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumServerReadLatencyMeasurements);

        double serverReadLatAvg = -1;
        if (SysStats::NumServerReadLatencyMeasurements != 0) {
            serverReadLatAvg = SysStats::ServerReadLatencySum / SysStats::NumServerReadLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) serverReadLatAvg);

        Utils::appendToCSVString(stateStr, (double) SysStats::ChainLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumChainLatencyMeasurements);

        double chainLatAvg = -1;
        if (SysStats::NumChainLatencyMeasurements != 0) {
            chainLatAvg = SysStats::ChainLatencySum / SysStats::NumChainLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) chainLatAvg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CohordHandleInternalSliceReadLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCohordHandleInternalSliceReadLatencyMeasurements);
        double avg = -1;
        if (SysStats::NumCohordHandleInternalSliceReadLatencyMeasurements != 0) {
            avg = SysStats::CohordHandleInternalSliceReadLatencySum /
                  SysStats::NumCohordHandleInternalSliceReadLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CohordSendingReadResultsLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCohordSendingReadResultsLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCohordSendingReadResultsLatencyMeasurements != 0) {
            avg = SysStats::CohordSendingReadResultsLatencySum /
                  SysStats::NumCohordSendingReadResultsLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorMapingKeysToShardsLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorMapingKeysToShardsLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorMapingKeysToShardsLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorMapingKeysToShardsLatencySum /
                  SysStats::NumCoordinatorMapingKeysToShardsLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorSendingReadReqToShardsLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorSendingReadReqToShardsLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorSendingReadReqToShardsLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorSendingReadReqToShardsLatencySum /
                  SysStats::NumCoordinatorSendingReadReqToShardsLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorReadingLocalKeysLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorReadingLocalKeysLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorReadingLocalKeysLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorReadingLocalKeysLatencySum /
                  SysStats::NumCoordinatorReadingLocalKeysLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorProcessingOtherShardsReadRepliesLatencySum);
        Utils::appendToCSVString(stateStr,
                                 (int) SysStats::NumCoordinatorProcessingOtherShardsReadRepliesLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorProcessingOtherShardsReadRepliesLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorProcessingOtherShardsReadRepliesLatencySum /
                  SysStats::NumCoordinatorProcessingOtherShardsReadRepliesLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorLocalKeyReadsLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorLocalKeyReadsLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorLocalKeyReadsLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorLocalKeyReadsLatencySum /
                  SysStats::NumCoordinatorLocalKeyReadsLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorWaitingForShardsRepliesLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorWaitingForShardsRepliesMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorWaitingForShardsRepliesMeasurements != 0) {
            avg = SysStats::CoordinatorWaitingForShardsRepliesLatencySum /
                  SysStats::NumCoordinatorWaitingForShardsRepliesMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorReadReplyHandlingLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorReadReplyHandlingMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorReadReplyHandlingMeasurements != 0) {
            avg = SysStats::CoordinatorReadReplyHandlingLatencySum /
                  SysStats::NumCoordinatorReadReplyHandlingMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorCommitLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorCommitMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorCommitMeasurements != 0) {
            avg = SysStats::CoordinatorCommitLatencySum /
                  SysStats::NumCoordinatorCommitMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorCommitMappingKeysToShardsLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorCommitMappingKeysToShardsLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorCommitMappingKeysToShardsLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorCommitMappingKeysToShardsLatencySum /
                  SysStats::NumCoordinatorCommitMappingKeysToShardsLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorCommitSendingPrepareReqToShardsLatencySum);
        Utils::appendToCSVString(stateStr,
                                 (int) SysStats::NumCoordinatorCommitSendingPrepareReqToShardsLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorCommitSendingPrepareReqToShardsLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorCommitSendingPrepareReqToShardsLatencySum /
                  SysStats::NumCoordinatorCommitSendingPrepareReqToShardsLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);


        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorCommitLocalPrepareLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorCommitLocalPrepareLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorCommitLocalPrepareLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorCommitLocalPrepareLatencySum /
                  SysStats::NumCoordinatorCommitLocalPrepareLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr,
                                 (double) SysStats::CoordinatorCommitWaitingOtherShardsPrepareRepliesLatencySum);
        Utils::appendToCSVString(stateStr,
                                 (int) SysStats::NumCoordinatorCommitWaitingOtherShardsPrepareRepliesLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorCommitWaitingOtherShardsPrepareRepliesLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorCommitWaitingOtherShardsPrepareRepliesLatencySum /
                  SysStats::NumCoordinatorCommitWaitingOtherShardsPrepareRepliesLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);


        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorCommitProcessingPrepareRepliesLatencySum);
        Utils::appendToCSVString(stateStr,
                                 (int) SysStats::NumCoordinatorCommitProcessingPrepareRepliesLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorCommitProcessingPrepareRepliesLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorCommitProcessingPrepareRepliesLatencySum /
                  SysStats::NumCoordinatorCommitProcessingPrepareRepliesLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::CoordinatorCommitSendingCommitReqLatencySum);
        Utils::appendToCSVString(stateStr, (int) SysStats::NumCoordinatorCommitSendingCommitReqLatencyMeasurements);
        avg = -1;
        if (SysStats::NumCoordinatorCommitSendingCommitReqLatencyMeasurements != 0) {
            avg = SysStats::CoordinatorCommitSendingCommitReqLatencySum /
                  SysStats::NumCoordinatorCommitSendingCommitReqLatencyMeasurements;
        }
        Utils::appendToCSVString(stateStr, (double) avg);

        Utils::appendToCSVString(stateStr, (double) SysStats::NumSentGSTs);
        Utils::appendToCSVString(stateStr, (double) SysStats::NumRecvGSTs);
        Utils::appendToCSVString(stateStr, (double) SysStats::NumSentGSTBytes);
        Utils::appendToCSVString(stateStr, (double) SysStats::NumRecvGSTBytes);

        Utils::appendToCSVString(stateStr, (double) SysStats::NumSentUSTs);
        Utils::appendToCSVString(stateStr, (double) SysStats::NumRecvUSTs);
        Utils::appendToCSVString(stateStr, (double) SysStats::NumSentUSTBytes);
        Utils::appendToCSVString(stateStr, (double) SysStats::NumRecvUSTBytes);

        Utils::appendToCSVString(stateStr, (double) SysConfig::NumChannelsPartition);

        /* visibility stats */
        for (int i = 0; i < 11; i++) {
            Utils::appendToCSVString(stateStr, (double) vs.overall[i]);
        }

        for (int i = 0; i < 11; i++) {
            Utils::appendToCSVString(stateStr, (double) vs.propagation[i]);
        }

        for (int i = 0; i < 11; i++) {
            Utils::appendToCSVString(stateStr, (double) vs.visibility[i]);
        }

        for (int i = 0; i < 11; i++) {
            Utils::appendToCSVString(stateStr, (double) vsLocal.overall[i]);
        }

        for (int i = 0; i < 11; i++) {
            Utils::appendToCSVString(stateStr, (double) vsLocal.propagation[i]);
        }

        for (int i = 0; i < 11; i++) {
            Utils::appendToCSVString(stateStr, (double) vsLocal.visibility[i]);
        }

        replace(stateStr.begin(), stateStr.end(), ',', separator);

        return true;

    }

    bool MVKVTXStore::DumpLatencyMeasurement(std::string &resultStr) {
        LOCK_PENDING_INVISIBLE_QUEUE();

            for (unsigned int i = 0; i < _recordedLatencies.size(); ++i) {

                ReplicationLatencyResult &r = _recordedLatencies[i];
                resultStr +=
                        std::to_string(r.OverallLatency.Seconds * 1000 +
                                       r.OverallLatency.NanoSeconds / 1000000.0) + " " +
                        std::to_string(r.PropagationLatency.Seconds * 1000 +
                                       r.PropagationLatency.NanoSeconds / 1000000.0) + " " +
                        std::to_string(r.VisibilityLatency.Seconds * 1000 +
                                       r.VisibilityLatency.NanoSeconds / 1000000.0) + " [ms] " +
                        std::to_string(r.SrcReplica) + "\n";
            }
        UNLOCK_PENDING_INVISIBLE_QUEUE();

        return true;
    }


    void MVKVTXStore::ShowVisibilityLatency(VisibilityStatistics &v, bool remoteMode) {
#ifdef MEASURE_VISIBILITY_LATENCY
        LOCK_PENDING_INVISIBLE_QUEUE();
            std::vector<double> overallLatency;
            std::vector<double> propagationLatency;
            std::vector<double> visibilityLatency;

            std::vector<ReplicationLatencyResult> _latencies;

            if (remoteMode) {
                _latencies = _recordedLatencies;
            } else {
                _latencies = _recordedLocalLatencies;
            }

            SLOG((boost::format("_latencies.size() = %d .") % _latencies.size()).str());
            if (_latencies.size() > 0) {
                for (unsigned int i = 0; i < _latencies.size(); ++i) {

                    ReplicationLatencyResult &r = _latencies[i];
                    overallLatency.push_back(r.OverallLatency.Seconds * 1000 +
                                             r.OverallLatency.NanoSeconds / 1000000.0);
                    propagationLatency.push_back(r.PropagationLatency.Seconds * 1000 +
                                                 r.PropagationLatency.NanoSeconds / 1000000.0);
                    visibilityLatency.push_back(r.VisibilityLatency.Seconds * 1000 +
                                                r.VisibilityLatency.NanoSeconds / 1000000.0);
                }
                std::sort(overallLatency.begin(), overallLatency.end());
                std::sort(propagationLatency.begin(), propagationLatency.end());
                std::sort(visibilityLatency.begin(), visibilityLatency.end());

                //We take 10-20-30-40-50-60-70-80-90-95-99-th percentile
                double size = overallLatency.size() / 100.0;
                int i;


                for (i = 1; i <= 9; i++) {
                    v.overall[i - 1] = overallLatency[(int) (i * 10.0 * size)];
                }
                v.overall[9] = overallLatency[(int) (95.0 * size)];
                v.overall[10] = overallLatency[(int) (99.0 * size)];

                for (i = 1; i <= 9; i++) {
                    v.propagation[i - 1] = propagationLatency[(int) (i * 10.0 * size)];
                }
                v.propagation[9] = propagationLatency[(int) (95.0 * size)];
                v.propagation[10] = propagationLatency[(int) (99.0 * size)];

                for (i = 1; i <= 9; i++) {
                    v.visibility[i - 1] = visibilityLatency[(int) (i * 10.0 * size)];
                }
                v.visibility[9] = visibilityLatency[(int) (95.0 * size)];
                v.visibility[10] = visibilityLatency[(int) (99.0 * size)];
            }

        UNLOCK_PENDING_INVISIBLE_QUEUE();
#endif


    }

    int MVKVTXStore::Size() {
        int size = 0;
        for (int i = 0; i < SysConfig::NumItemIndexTables; i++) {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[i]));
            size += _indexTables[i]->size();
        }

        return size;
    }

    void MVKVTXStore::Reserve(int numItems) {
        for (int i = 0; i < SysConfig::NumItemIndexTables; i++) {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[i]));
            _indexTables[i]->reserve(numItems / SysConfig::NumItemIndexTables + 1);
        }
    }

//////////////////////////////////////////////////////

    bool MVKVTXStore::HandlePropagatedUpdate(std::vector<PropagatedUpdate *> &updates) {
        for (unsigned int i = 0; i < updates.size(); i++) {
            InstallPropagatedUpdate(updates[i]);
        }

#ifdef MEASURE_STATISTICS
        SysStats::NumReceivedPropagatedUpdates += updates.size();
        SysStats::NumReceivedBatches += 1;
#endif

        return true;
    }


    bool MVKVTXStore::HandleHeartbeat(Heartbeat &hb, int srcReplica) {
        LOCK_VV(srcReplica);

            assert(hb.PhysicalTime >= _VV(srcReplica)); //_PVV does not play role in the hybrid clock version
            assert(hb.LogicalTime >= _LVV[_ridToVVIndex[srcReplica]]);
            _VV(srcReplica) = hb.PhysicalTime;
            assert(_VV_LATEST(srcReplica) <= _VV(srcReplica));
            _VV_LATEST(srcReplica) = _VV(srcReplica);
            _LVV[_ridToVVIndex[srcReplica]] = hb.LogicalTime;
        UNLOCK_VV(srcReplica);
        return true;
    }

    void MVKVTXStore::InstallPropagatedUpdate(PropagatedUpdate *update) {
        ItemAnchor *itemAnchor = NULL;
        PhysicalTimeSpec depTime;

        int ti = Utils::strhash(update->Key) % SysConfig::NumItemIndexTables;
        if (ti < 0) ti += SysConfig::NumItemIndexTables;

        {
            std::lock_guard<std::mutex> lk(*(_indexTableMutexes[ti]));
            try {
                itemAnchor = _indexTables[ti]->at(update->Key);
            } catch (std::out_of_range &e) {
                if (SysConfig::Debugging) {
                    SLOG((boost::format("MVKVTXStore: ApplyPropagatedUpdate key %s does not exist.")
                          % update->Key).str());
                    assert(false);
                }
            }
        }

        // create and insert propagated version
        // no need to delete

        ItemVersion *newVersion = new ItemVersion(update->Value);
        newVersion->Key = update->Key;
        newVersion->UT = update->UT;
        newVersion->SrcReplica = update->SrcReplica;
        newVersion->Persisted = false;
        newVersion->LUT = update->LUT;
#ifdef MEASURE_VISIBILITY_LATENCY
        newVersion->CreationTime = update->CreationTime;
#endif

        update->UpdatedItemAnchor = itemAnchor;
        update->UpdatedItemVersion = newVersion;

        newVersion->RIT = Utils::GetCurrentClockTime();// RIT is always physical time

        itemAnchor->InsertVersion(newVersion);

#ifdef MEASURE_STATISTICS
        SysStats::NumPendingPropagatedUpdates += 1;
#endif

        newVersion->Persisted = true;

        LOCK_VV(update->SrcReplica);
            _replicaClock += 1;
            _LVV[_ridToVVIndex[update->SrcReplica]] = update->LUT;
            assert(_VV(update->SrcReplica) <= update->UT);

#ifndef EVENTUAL_CONSISTENCY
            if (_VV(update->SrcReplica) > update->UT) {
                //inserting not in FIFO //the updates could be from the same transaction with the same commit time
                int64_t u_l, u_p, c_l, c_p;
                HYB_TO_PHYS(u_p, update->UT.Seconds);
                HYB_TO_PHYS(c_p, _VV(update->SrcReplica).Seconds);
                HYB_TO_LOG(u_l, update->UT.Seconds);
                HYB_TO_LOG(c_l, _VV(update->SrcReplica).Seconds);
                fprintf(stdout, "[INFO]:Local: %d.%d vs update %d.%d\n", c_p, c_l, u_p, u_l);
                fflush(stdout);
                assert(false);
            }
#endif //EVENTUAL_CONSISTENCY

            _VV(update->SrcReplica) = update->UT;
            assert(_VV_LATEST(update->SrcReplica) <= _VV(update->SrcReplica));
            _VV_LATEST(update->SrcReplica) = _VV(update->SrcReplica);
        UNLOCK_VV(update->SrcReplica);


        delete update;

#ifdef MEASURE_VISIBILITY_LATENCY
        LOCK_PENDING_INVISIBLE_QUEUE();
            _pendingInvisibleVersions[_ridToVVIndex[newVersion->SrcReplica]].push(newVersion);
        UNLOCK_PENDING_INVISIBLE_QUEUE();
#endif
    }


    void MVKVTXStore::CheckAndRecordVisibilityLatency(PhysicalTimeSpec ust) {
#ifdef MEASURE_VISIBILITY_LATENCY

        PhysicalTimeSpec currentTime = Utils::GetCurrentClockTime();

        LOCK_PENDING_INVISIBLE_QUEUE();

            PhysicalTimeSpec rit;
            PhysicalTimeSpec put;
            int j;
            bool vis;

            for (unsigned int i = 0; i < _pendingInvisibleVersions.size(); ++i) {
                int srcRID = _ridToVVIndex[i];
                std::queue<ItemVersion *> &q = _pendingInvisibleVersions[i];
                vis = true;
                while (!q.empty()) {
                    ItemVersion *version = q.front();

                    if (version->UT > ust) {
                        vis = false;
                    }

                    if (!vis) {
                        break;
                    }

                    _numCheckedVersions += 1;

                    if (_numCheckedVersions % SysConfig::LatencySampleInterval == 0) {
                        put = version->CreationTime;
                        PhysicalTimeSpec put64 = put;
                        FROM_64(put64, put64);

                        ReplicationLatencyResult result;
                        result.OverallLatency = currentTime - put64;
                        result.PropagationLatency = version->RIT - put64;
                        result.VisibilityLatency = currentTime - version->RIT;
                        result.SrcReplica = version->SrcReplica;

                        _recordedLatencies.push_back(result);

                    }
                    q.pop();
                }
            }

        UNLOCK_PENDING_INVISIBLE_QUEUE();
#endif
    }


    void MVKVTXStore::calculateStalenessStatistics(StalenessStatistics &stat) {

        {
            std::lock_guard<std::mutex> lk(SysStats::NumFresherVersionsInItemChainMutex);
            std::vector<int64_t> numFreshVer = SysStats::NumFresherVersionsInItemChain;

            if (numFreshVer.size() == 0) {
                stat.averageNumFresherVersionsInItemChain = 0;
            } else {
                stat.averageNumFresherVersionsInItemChain = std::accumulate(
                        numFreshVer.begin(), numFreshVer.end(), 0.0) / numFreshVer.size();
                assert(stat.averageNumFresherVersionsInItemChain >= 1);
            }
        }

        std::vector<double> usrPercST;

        {
            std::lock_guard<std::mutex> lk(SysStats::UserPerceivedStalenessTimeMutex);
            usrPercST = SysStats::UserPerceivedStalenessTime;
        }

        if (usrPercST.size() == 0) {
            stat.averageUserPerceivedStalenessTime = 0;
            stat.minStalenessTime = 0;
            stat.maxStalenessTime = 0;
            stat.medianStalenessTime = 0;
            stat._90PercentileStalenessTime = 0;
            stat._95PercentileStalenessTime = 0;
            stat._99PercentileStalenessTime = 0;

        } else {

            stat.averageUserPerceivedStalenessTime =
                    std::accumulate(usrPercST.begin(), usrPercST.end(), 0.0) / usrPercST.size();

            std::sort(usrPercST.begin(), usrPercST.end());

            stat.minStalenessTime = SysStats::MinUserPercievedStalenessTime;
            stat.maxStalenessTime = SysStats::MaxUserPercievedStalenessTime;
            stat.medianStalenessTime = usrPercST.at((int) std::round(usrPercST.size() * 0.50));
            stat._90PercentileStalenessTime = usrPercST.at((int) std::round(usrPercST.size() * 0.90) - 1);
            stat._95PercentileStalenessTime = usrPercST.at((int) std::round(usrPercST.size() * 0.95) - 1);
            stat._99PercentileStalenessTime = usrPercST.at((int) std::round(usrPercST.size() * 0.99) - 1);
        }

    }

    void MVKVTXStore::addStalenessTimesToOutput(string &str) {
        {
            str += "\n\nSTALENESS TIMES\n\n";
            std::lock_guard<std::mutex> lk(SysStats::UserPerceivedStalenessTimeMutex);

            for (int i = 0; i < SysStats::UserPerceivedStalenessTime.size(); i++) {
                str += ((boost::format("%lf\n") % SysStats::UserPerceivedStalenessTime[i]).str());
            }

        }

    }

    PhysicalTimeSpec MVKVTXStore::GetVV(int replicaId) {

        return _VV(replicaId);

    }

    std::vector<PhysicalTimeSpec> MVKVTXStore::GetVV() {
        return _HVV;
    }

    std::vector<int64_t> MVKVTXStore::GetLVV() {
        return _LVV;
    }

    int64_t MVKVTXStore::getLocalClock() {
        return _localClock;
    }


    void MVKVTXStore::getCurrentGlobalValue(PhysicalTimeSpec &global) {
        std::vector<PhysicalTimeSpec> currVV = _HVV;
        PhysicalTimeSpec min(INT64_MAX, INT64_MAX);

        for (int i = 0; i < currVV.size(); i++) {
            int index = _ridToVVIndex[i];
            if (currVV[index] < min) {
                min = currVV[index];
            }
        }

        global = min;
    }

    PhysicalTimeSpec MVKVTXStore::getVVmin() {
        PhysicalTimeSpec min;
        min = _HVV[0];

        for (int i = 0; i < _HVV.size(); i++) {
            if (_HVV[i] < min) {
                min = _HVV[i];
            }
        }
        return min;
    }

    void MVKVTXStore::updateVV(int replicaId, PhysicalTimeSpec t) {
        LOCK_VV(replicaId);
            _VV(replicaId) = MAX(t, _VV(replicaId));
            assert(_VV_LATEST(replicaId) <= _VV(replicaId));
            _VV_LATEST(replicaId) = _VV(replicaId);
        UNLOCK_VV(replicaId);


    }

    PhysicalTimeSpec MVKVTXStore::updateAndGetVV(int replicaId, PhysicalTimeSpec t) {
        PhysicalTimeSpec res;

        LOCK_VV(replicaId);
            _VV(replicaId) = MAX(t, _VV(replicaId));
            assert(_VV_LATEST(replicaId) <= _VV(replicaId));
            res = _VV(replicaId);
            _VV_LATEST(replicaId) = _VV(replicaId);
        UNLOCK_VV(replicaId);


        return res;

    }

    std::vector<PhysicalTimeSpec> MVKVTXStore::updateAndGetVV() {

        std::vector<PhysicalTimeSpec> ret;
        PhysicalTimeSpec now = Utils::GetCurrentClockTime();
        LOCK_VV(_replicaId);
            TO_64(now, now);
            SET_HYB_PHYS(now.Seconds, now.Seconds);
            SET_HYB_LOG(now.Seconds, 0);
            if (now.Seconds > _VV(_replicaId).Seconds) {
                _VV(_replicaId) = now;
                assert(_VV_LATEST(_replicaId) <= _VV(_replicaId));
                _VV(_replicaId) = _VV(_replicaId);
            }
            ret = _HVV;
        UNLOCK_VV(_replicaId);
        return ret;
    }


    PhysicalTimeVector MVKVTXStore::getGSTsVector() {
#ifndef SIXTY_FOUR_BIT_CLOCK
        std::lock_guard<std::mutex> lk(_GSTMutex);
#endif
        return _GSTs;
    }

    PhysicalTimeSpec MVKVTXStore::GetGSTforDCId(int dcID) {
#ifndef SIXTY_FOUR_BIT_CLOCK
        std::lock_guard<std::mutex> lk(_GSTMutex);
#endif
        return _GSTs[dcID];
    }


    void MVKVTXStore::SetGSTatDCId(PhysicalTimeSpec gst, int index) {
        std::lock_guard<std::mutex> lk(_GSTMutex);
        assert(_GSTs[index] <= gst);
        _GSTs[index] = gst;
    }

    void MVKVTXStore::HandleGSTPushFromPeerDC(PhysicalTimeSpec gst, int srcDC) {
        std::lock_guard<std::mutex> lk(_GSTMutex);
        _GSTs[srcDC] = MAX(_GSTs[srcDC], gst);

    }

    void MVKVTXStore::UpdateUST() {

        if (_GSTs.size() == 1) { //FIXME: This case could be buggy
            PhysicalTimeSpec gst;
            {
                std::lock_guard<std::mutex> lk(_GSTMutex);
                gst = _GSTs[0];
            }

            std::lock_guard<std::mutex> lk(_USTMutex);

            _UST = MAX(_UST, gst);
            SLOG("GSVs size is 1\n");

            return;
        }

        PhysicalTimeSpec ust;

        LOCK_GSTs()
            ust = _GSTs[0];

            for (int i = 1; i < _GSTs.size(); i++) {
                ust = MIN(ust, _GSTs[i]);
            }

        UNLOCK_GSTs()

        assert(ust <= GetVV(_replicaId));

        if (ust > _UST) {
            LOCK_UST();

                _UST = MAX(_UST, ust); //Never move the UST back in time
            UNLOCK_UST();

#ifdef MEASURE_VISIBILITY_LATENCY
            CheckAndRecordVisibilityLatency(_UST);
#endif
        }

    }

    PhysicalTimeSpec MVKVTXStore::getUST() {
        return _UST;
    }

    void MVKVTXStore::setUST(scc::PhysicalTimeSpec &ust) {
        LOCK_UST();
            assert(ust <= LOCAL_VV_ENTRY);
            _UST = ust;
        UNLOCK_UST();
    }

    void MVKVTXStore::updateUSTifSmaller(scc::PhysicalTimeSpec ust) {

        if (_UST < ust) {
            LOCK_UST();

                assert(ust <= LOCAL_VV_ENTRY);
                _UST = MAX(_UST, ust);
            UNLOCK_UST();

#ifdef MEASURE_VISIBILITY_LATENCY
            //            SLOG("Calling CheckAndRecordVisibilityLatency");
                        CheckAndRecordVisibilityLatency(_UST);
#endif
        }

    }

    void MVKVTXStore::updateGSTifSmaller(scc::PhysicalTimeSpec gst) {

        if (_GSTs[_replicaId] < gst) {
            LOCK_GSTs();

                assert(gst <= LOCAL_VV_ENTRY);
                _GSTs[_replicaId] = MAX(_GSTs[_replicaId], gst);

            UNLOCK_GSTs();

        }

    }

} // namespace scc
