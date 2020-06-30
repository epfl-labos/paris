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



#ifndef SCC_KVSTORE_MV_KVSTORE_H_
#define SCC_KVSTORE_MV_KVSTORE_H_

#include "common/utils.h"
#include "common/sys_stats.h"
#include "common/sys_config.h"
#include "common/sys_logger.h"
#include "common/types.h"
#include "kvstore/item_version.h"
#include "kvstore/item_anchor.h"
#include "kvstore/log_manager.h"
#include "kvservice/coordinator.h"
#include "messages/op_log_entry.pb.h"
#include <assert.h>
#include <type_traits>
#include <stdexcept>
#include <atomic>
#include <vector>
#include <unordered_map>
#include <queue>
#include <thread>
#include <chrono>
#include <numeric>
#include <iostream>

#if 0  //Apparently, also here using a spinlock degrades performance w.r.t. using a mutex
#error No spinlock
#define LOCK_HVV(A) do{\
                        while(!__sync_bool_compare_and_swap((volatile unsigned int *)&_hvv_l[A], 0, 1)){\
                                __asm __volatile("pause\n": : :"memory");\
                            }\
                    }while(0)

#define UNLOCK_HVV(A)     *(volatile unsigned int *)&_hvv_l[A] = 0
#else

#define LOCK_HVV(A)  { std::lock_guard <std::mutex> lk(_hvvMutex[_ridToVVIndex[A]]);

#define UNLOCK_HVV(A)  }
#endif


#define PHYS_BITS 48
#define LOG_BITS 16
#define MAX_LOG 65536

//TODO: when recovering the nsec from a hybrid, you have to take into account that the last log bits from the leas significant 32 bits
//TODO: of the hybrid representation are logical, so you have to set them to 0
#define PHYS_MASK 0xFFFFFFFFFFFFFFFF<<LOG_BITS
#define LOG_MASK  0xFFFFFFFFFFFFFFFF>>PHYS_BITS

#define PHYS_TO_HYB(A, B) A = (B&PHYS_MASK)

#define HYB_TO_PHYS(A, B) PHYS_TO_HYB(A,B)
#define HYB_TO_LOG(A, B) A = (B&LOG_MASK)

#define SET_HYB_PHYS(A, B) A = (B & PHYS_MASK) | (A & LOG_MASK)
#define SET_HYB_LOG(A, B) assert(B < MAX_LOG); A = (B & LOG_MASK) | (A & PHYS_MASK)

#define LOCK_VV(A) LOCK_HVV(A)
#define UNLOCK_VV(A) UNLOCK_HVV(A)


#define _VV(A) _HVV[_ridToVVIndex[A]]
#define _VV_LATEST(A) _HVV_latest[_ridToVVIndex[A]]

#define LOCK_GSTs() {std::lock_guard <std::mutex> lk(_GSTMutex);
#define UNLOCK_GSTs() }

#define LOCK_UST() {std::lock_guard <std::mutex> lk(_USTMutex);
#define UNLOCK_UST() }

#define LOCAL_VV_ENTRY MVKVTXStore::Instance()->GetVV(_replicaId)

#define LOCK_PENDING_INVISIBLE_QUEUE() { std::lock_guard<std::mutex> lk(_pendingInvisibleVersionsMutex);
#define UNLOCK_PENDING_INVISIBLE_QUEUE() }


namespace scc {

    typedef std::unordered_map<std::string, ItemAnchor *> ItemIndexTable;

    class MVKVTXStore {

    public:

        static MVKVTXStore *Instance();

        MVKVTXStore();

        ~MVKVTXStore();

        void SetPartitionInfo(DBPartition p, int numPartitions, int numPartitionsInLocalDC, int numReplicasPerPartition,
                              int numDCs,
                              std::unordered_map<int, int> &ridToIndex, std::vector<int> &localPartIDs);

        void Initialize();

        void GetSysClockTime(PhysicalTimeSpec &physicalTime, int64_t &logicalTime);

        bool Add(const std::string &key, const std::string &value);

        bool AddAgain(const std::string &key, const std::string &value);


        bool Write(PhysicalTimeSpec commitTime, TxContex &cdata, const std::string &key, const std::string &value,
                   std::vector<LocalUpdate *> &updatesToBeReplicatedQueue);


        bool LocalTxSliceGet(const TxContex &cdata, const std::string &key,
                             std::string &value);

#ifdef BLOCKING_ON
        void WaitOnTxSlice(unsigned int txId,  TxContex &cdata);
#endif

        bool ShowItem(const std::string &key, std::string &itemVersions);

        bool ShowDB(std::string &allItemVersions);

        bool ShowTime(std::string &stateStr);

        bool ShowStateTx(std::string &stateStr);

        bool ShowStateTxCSV(std::string &stateStr);

        bool DumpLatencyMeasurement(std::string &resultStr);

        int Size();

        void Reserve(int numItems);

        bool HandlePropagatedUpdate(std::vector<PropagatedUpdate *> &updates);

        bool HandleHeartbeat(Heartbeat &hb, int srcReplica);

        std::vector<int64_t> GetLVV();

        int64_t getLocalClock();

        PhysicalTimeSpec getVVmin();

        PhysicalTimeSpec GetVV(int replicaId);

        std::vector<PhysicalTimeSpec> GetVV();

        void updateVV(int replicaId, PhysicalTimeSpec t);

        std::vector<PhysicalTimeSpec> updateAndGetVV();

        PhysicalTimeSpec updateAndGetVV(int replicaId, PhysicalTimeSpec t);

        void getCurrentGlobalValue(PhysicalTimeSpec &global);

        PhysicalTimeSpec GetUST();

        void SetGSTatDCId(PhysicalTimeSpec gst, int index);

        PhysicalTimeSpec GetGSTforDCId(int dcID);

        PhysicalTimeVector getGSTsVector();

        inline int getReplicaId() {
            return _replicaId;
        }

        inline int getPartitionId() {
            return _partitionId;
        }

        void CheckAndRecordVisibilityLatency(PhysicalTimeSpec ust);

        void HandleGSTPushFromPeerDC(PhysicalTimeSpec gst, int srcDC);

        void UpdateUST();

        PhysicalTimeSpec getUST();


        void setUST(scc::PhysicalTimeSpec &ust);

        void updateUSTifSmaller(PhysicalTimeSpec ust);

        void updateGSTifSmaller(PhysicalTimeSpec gst);


    private:

        void ShowVisibilityLatency(VisibilityStatistics &v, bool remoteMode);

        // update propagation
        void InstallPropagatedUpdate(PropagatedUpdate *update);


        void calculateStalenessStatistics(StalenessStatistics &stat);

        void addStalenessTimesToOutput(string &str);

        template<typename T>
        std::string addToStateStr(const std::string property, T value) {

            std::string str;

            if (std::is_same<T, std::string>::value) {
                str = (boost::format(" %s %s\n") % property % value).str();
            } else {
                str = (boost::format(" %s %s\n") % property % std::to_string(value)).str();
            }

            return str;
        }

        std::string addToStateStr(const std::string property, std::string value) {

            return (boost::format(" %s %s\n") % property % value).str();
        }


    public:

        volatile int _hvv_l[32];
        std::mutex _hvvMutex[32];

        std::vector<PhysicalTimeSpec> _HVV; // hybrid version vector
        std::vector<PhysicalTimeSpec> _HVV_latest; // hybrid version vector

        std::unordered_map<int, int> _ridToVVIndex; //replica id to version vector index
        std::unordered_map<int, int> _pidToGSVIndex; //partition id to stabilization vector index

        // measure replication visibility latency
        std::mutex _pendingInvisibleVersionsMutex;
        std::vector<std::queue<ItemVersion *>> _pendingInvisibleVersions;
        std::vector<ReplicationLatencyResult> _recordedLatencies;
        std::vector<ReplicationLatencyResult> _recordedLocalLatencies;
        int _numCheckedVersions;


    private:

        std::vector<ItemIndexTable *> _indexTables;
        std::vector<std::mutex *> _indexTableMutexes;

        //general info
        int _partitionId;
        int _replicaId;
        int _numPartitions;
        int _numPartitionsInLocalDC;
        int _numReplicasPerPartition;
        int _numDCs;

        // system clock
        int64_t _localClock;
        int64_t _replicaClock;

        std::vector<int64_t> _LVV; // logical version vector (for debugging)
        PhysicalTimeSpec _initItemVersionUT; // PUT of initially loaded item version

        std::mutex _GSTMutex;
        std::vector<std::mutex *> _GSTsMutexes;
        std::vector<PhysicalTimeSpec> _GSTs;

        PhysicalTimeSpec _UST;
        std::mutex _USTMutex;

    };


} // namespace scc

#endif
