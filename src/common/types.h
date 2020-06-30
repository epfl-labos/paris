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

#ifndef SCC_COMMON_TYPES_H
#define SCC_COMMON_TYPES_H

#include "common/wait_handle.h"
#include "common/sys_logger.h"
#include <sys/types.h>
#include <algorithm>
#include <string>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <functional>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cmath>
#include <cassert>
#include <inttypes.h>
#include <limits>
#include <queue>
#include <ostream>

namespace scc {

    class Configuration {
    public:
        int NumPartitions;
        int NumDataCenters;
        int ReplicationFactor;
        int TotalNumItems;
        int NumValueBytes;
        int NumOpsPerThread;
        int NumTxsPerThread;
        int NumThreads;
        int ServingReplicaId;
        int ServingPartitionId;
        int ClientServerId;
        std::string RequestsDistribution;
        double RequestsDistributionParameter;
        std::string GroupServerName;
        int GroupServerPort;
        int ReadRatio;
        int WriteRatio;
        int LOTxRatio;
        int LRTxRatio;
        int ReadTxRatio;
        std::string clientServerName;
        std::string expResultsOutputFileName;
        bool reservoirSampling;
        int reservoirSamplingLimit;
        int experimentDuration; //milliseconds
        int warmUpDuration; //milliseconds
        int TotalNumOps;
        int TotalNumTxs;
        int experimentType; //1- experiment1, 2 -experiment2
        int clientResetNumber;
        bool enableClientReset;
        int numTxReadItems;
        int numTxWriteItems;
        int locality;
        int64_t TotalNumReadItems;
        int64_t numItemsReadFromClientWriteSet;
        int64_t numItemsReadFromClientReadSet;
        int64_t numItemsReadFromClientWriteCache;
        int64_t numItemsReadFromStore;
        int64_t totalNumClientReadItems;
        int missingThreads;

    };

    class PhysicalTimeSpec {
    public:

        PhysicalTimeSpec() : Seconds(0), NanoSeconds(0) {
        }

        PhysicalTimeSpec(int64_t seconds, int64_t nanoSeconds)
                : Seconds(seconds),
                  NanoSeconds(nanoSeconds) {
        }

        PhysicalTimeSpec(const PhysicalTimeSpec &other) {
            this->NanoSeconds = other.NanoSeconds;
            this->Seconds = other.Seconds;

        }

        PhysicalTimeSpec &operator=(const PhysicalTimeSpec &other) {
            this->NanoSeconds = other.NanoSeconds;
            this->Seconds = other.Seconds;

        }

        bool Zero() {
            return Seconds == 0 && NanoSeconds == 0;
        }

        int64_t Seconds;
        int64_t NanoSeconds;

        double toMilliSeconds();

        double toMicroSeconds();

        void addMilliSeconds(double ms);

        double toSeconds();

        void setToZero() {
            Seconds = 0;
            NanoSeconds = 0;
        }

    };

    bool operator==(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return (a.Seconds == b.Seconds) && (a.NanoSeconds == b.NanoSeconds);
    }

    bool operator!=(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return !(a == b);
    }

    bool operator>(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return (a.Seconds > b.Seconds) ||
               ((a.Seconds == b.Seconds) && (a.NanoSeconds > b.NanoSeconds));
    }

    bool operator<=(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return !(a > b);
    }

    bool operator<(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return (a.Seconds < b.Seconds) ||
               ((a.Seconds == b.Seconds) && (a.NanoSeconds < b.NanoSeconds));
    }

    bool operator>=(const PhysicalTimeSpec &a, const PhysicalTimeSpec &b) {
        return !(a < b);
    }

    PhysicalTimeSpec operator-(PhysicalTimeSpec &a, PhysicalTimeSpec &b) {
        PhysicalTimeSpec d;

        d.Seconds = a.Seconds - b.Seconds;
        d.NanoSeconds = a.NanoSeconds - b.NanoSeconds;

        while (d.NanoSeconds < 0) {
            d.Seconds -= 1;
            d.NanoSeconds += 1000000000;
        }

        return d;
    }

    double PhysicalTimeSpec::toMilliSeconds() {
        double res = 0;

        res = (double) this->NanoSeconds / 1000000.0;
        res += this->Seconds * 1000.0;

        return res;
    }

    void PhysicalTimeSpec::addMilliSeconds(double ms) {
        this->NanoSeconds += ms * 1000000;
    }

    double PhysicalTimeSpec::toMicroSeconds() {
        double res = 0;

        res = (double) this->NanoSeconds / 1000.0;
        res += this->Seconds * 1000000.0;

        return res;
    }

    double PhysicalTimeSpec::toSeconds() {
        double res = 0;

        res = this->Seconds + (double) this->NanoSeconds / 1000000000.0;

        return res;
    }

    enum class DurabilityType {
        Disk = 1,
        Memory
    };

    enum class GSTDerivationType {
        TREE = 1,
        BROADCAST,
        SIMPLE_BROADCAST
    };

    class DBPartition {
    public:

        DBPartition()
                : Name("NULL"),
                  PublicPort(-1),
                  PartitionPort(-1),
                  ReplicationPort(-1),
                  PartitionId(-1),
                  ReplicaId(-1),
                  NodeId(-1),
                  isLocal(true) {
        }

        DBPartition(std::string name, int publicPort)
                : Name(name),
                  PublicPort(publicPort),
                  PartitionPort(-1),
                  ReplicationPort(-1),
                  PartitionId(-1),
                  ReplicaId(-1),
                  NodeId(-1),
                  isLocal(true) {
        }

        DBPartition(std::string name,
                    int publicPort,
                    int partitionPort,
                    int replicationPort,
                    int partitionId,
                    int replicaId,
                    int nodeId,
                    bool local)
                : Name(name),
                  PublicPort(publicPort),
                  PartitionPort(partitionPort),
                  ReplicationPort(replicationPort),
                  PartitionId(partitionId),
                  ReplicaId(replicaId),
                  NodeId(nodeId),
                  isLocal(local) {
        }

        DBPartition(std::string name,
                    int publicPort,
                    int partitionPort,
                    int replicationPort,
                    int partitionId,
                    int replicaId,
                    bool local)
                : Name(name),
                  PublicPort(publicPort),
                  PartitionPort(partitionPort),
                  ReplicationPort(replicationPort),
                  PartitionId(partitionId),
                  ReplicaId(replicaId),
                  isLocal(local),
                  NodeId(-1) {
        }

        DBPartition(std::string name,
                    int publicPort,
                    int partitionPort,
                    int replicationPort,
                    int partitionId,
                    int replicaId)
                : Name(name),
                  PublicPort(publicPort),
                  PartitionPort(partitionPort),
                  ReplicationPort(replicationPort),
                  PartitionId(partitionId),
                  ReplicaId(replicaId),
                  isLocal(true),
                  NodeId(-1) {
        }

        DBPartition(std::string name,
                    int publicPort,
                    int partitionPort,
                    int replicationPort,
                    int partitionId,
                    int replicaId,
                    int nodeId)
                : Name(name),
                  PublicPort(publicPort),
                  PartitionPort(partitionPort),
                  ReplicationPort(replicationPort),
                  PartitionId(partitionId),
                  ReplicaId(replicaId),
                  NodeId(nodeId),
                  isLocal(true) {
        }

        DBPartition(const DBPartition &other) {
            this->Name = other.Name;
            this->ReplicaId = other.ReplicaId;
            this->PartitionId = other.PartitionId;
            this->PartitionPort = other.PartitionPort;
            this->ReplicationPort = other.ReplicationPort;
            this->PublicPort = other.PublicPort;
            this->isLocal = other.isLocal;
            this->NodeId = other.NodeId;

        }

        DBPartition &operator=(const DBPartition &other) {
            this->Name = other.Name;
            this->ReplicaId = other.ReplicaId;
            this->PartitionId = other.PartitionId;
            this->PartitionPort = other.PartitionPort;
            this->ReplicationPort = other.ReplicationPort;
            this->PublicPort = other.PublicPort;
            this->isLocal = other.isLocal;
            this->NodeId = other.NodeId;
        }

        std::string toString() {
            std::string loc = "True";
            if (!isLocal) loc = "False";

            std::string remoteUsage;
            for (int i = 0; i < adopteeNodesIndexes.size(); i++) {
                remoteUsage += (boost::format(
                        "=> PartitionID: %d ReplicaId: %d |") % adopteeNodesIndexes[i].first %
                                adopteeNodesIndexes[i].second).str();
            }
            return (boost::format(
                    "\nDBPartition => Name: %s | NodeId: %d | PartitionId: %d | ReplicaId: %d | Local:%s|Used Remotely by %s.\n") %
                    Name % NodeId % PartitionId % ReplicaId % loc % remoteUsage).str();
        }


    public:
        std::string Name;
        int PublicPort;
        int PartitionPort;
        int ReplicationPort;
        int PartitionId;
        int ReplicaId;
        bool isLocal;
        int NodeId;
        std::vector<std::pair<int, int>> adopteeNodesIndexes;
    };

    bool operator==(const DBPartition &lhs, const DBPartition &rhs) {
        bool areEqual = ((lhs.Name == rhs.Name) && (lhs.PartitionId == rhs.PartitionId) &&
                         (lhs.ReplicaId == rhs.ReplicaId));
        return areEqual;
    }

    typedef std::vector<int64_t> LogicalTimeVector;
    typedef std::vector<std::vector<int64_t>> LogicalTimeVectorVector;

    typedef std::vector<PhysicalTimeSpec> PhysicalTimeVector;
    typedef std::vector<std::vector<PhysicalTimeSpec>> PhysicalTimeVectorVector;

    bool operator<(const LogicalTimeVectorVector &a,
                   const LogicalTimeVectorVector &b) {
        bool leq = true;
        bool strictLess = false;
        for (unsigned int i = 0; i < a.size(); i++) {
            for (unsigned int j = 0; j < a[i].size(); j++) {
                if (a[i][j] < b[i][j]) {
                    strictLess = true;
                } else if (a[i][j] == b[i][j]) {
                    // do nothing
                } else {
                    leq = false;
                    break;
                }
            }
        }

        return (leq && strictLess);
    }

    bool operator>(const LogicalTimeVectorVector &a,
                   const LogicalTimeVectorVector &b) {
        bool geq = true;
        bool strictGreater = false;
        for (unsigned int i = 0; i < a.size(); i++) {
            for (unsigned int j = 0; j < a[i].size(); j++) {
                if (a[i][j] > b[i][j]) {
                    strictGreater = true;
                } else if (a[i][j] == b[i][j]) {
                    // do nothing
                } else {
                    geq = false;
                    break;
                }
            }
        }

        return (geq && strictGreater);
    }

    class ReplicaNode {
        int replicaId;
        int refCounter;
        int distance;

    public:
        ReplicaNode(int replicaId) : replicaId(replicaId), refCounter(0), distance(-1) {}

        int getReplicaId() const {
            return replicaId;
        }

        void increaseRefCounter() {
            refCounter++;
        }

        void setRefCounter(int refCounter) {
            ReplicaNode::refCounter = refCounter;
        }

        void setDistance(int distance) {
            ReplicaNode::distance = distance;
        }

        ReplicaNode(const ReplicaNode &other) {
            this->replicaId = other.replicaId;
            this->refCounter = other.refCounter;
            this->distance = other.distance;
        }

        bool operator==(const ReplicaNode &rhs) const {
            return replicaId == rhs.replicaId &&
                   refCounter == rhs.refCounter &&
                   distance == rhs.distance;
        }

        bool operator<(const ReplicaNode &rhs) const {
            return refCounter < rhs.refCounter;
        }

        bool operator>(const ReplicaNode &rhs) const {
            return rhs < *this;
        }

        bool operator<=(const ReplicaNode &rhs) const {
            return !(rhs < *this);
        }

        bool operator>=(const ReplicaNode &rhs) const {
            return !(*this < rhs);
        }

        bool operator!=(const ReplicaNode &rhs) const {
            return !(rhs == *this);
        }

        friend std::ostream &operator<<(std::ostream &os, const ReplicaNode &node) {
            os << "replicaId: " << node.replicaId << " refCounter: " << node.refCounter << " distance: "
               << node.distance
               << "\n";
            return os;
        }
    };

    struct CompareReplicaNodes {
        bool operator()(ReplicaNode lhs, ReplicaNode rhs) const {
            return lhs > rhs;
        }
    };

    class ConvenienceMetrics {
        int ref_counter;
        int distance;
    public:
        ConvenienceMetrics() : distance(-1), ref_counter(0) {}

        bool operator==(const ConvenienceMetrics &rhs) const {
            return ref_counter == rhs.ref_counter &&
                   distance == rhs.distance;
        }

        ConvenienceMetrics(const ConvenienceMetrics &other) {
            this->ref_counter = other.ref_counter;
            this->distance = other.distance;
        }

        ConvenienceMetrics &operator=(const ConvenienceMetrics &other) {
            this->ref_counter = other.ref_counter;
            this->distance = other.distance;
        }

        bool operator!=(const ConvenienceMetrics &rhs) const {
            return !(rhs == *this);
        }

        bool operator<(const ConvenienceMetrics &rhs) const {
            return ref_counter < rhs.ref_counter;
        }

        bool operator>(const ConvenienceMetrics &rhs) const {
            return rhs < *this;
        }

        bool operator<=(const ConvenienceMetrics &rhs) const {
            return !(rhs < *this);
        }

        bool operator>=(const ConvenienceMetrics &rhs) const {
            return !(*this < rhs);
        }


    };


    enum class ConsistencyType {
        Causal = 1
    };

    class ReadItemVersion {
    public:
        std::string Value;
        int64_t LUT;
        int SrcReplica;
        int SrcPartition;
    };

    class GetTransaction {
    public:
        std::vector<std::string> ToReadKeys;
        std::vector<ReadItemVersion> ReadVersions;
    };

    class ConsistencyMetadata {
    public:

        PhysicalTimeSpec DT;
        PhysicalTimeSpec GST;
        PhysicalTimeSpec DUT;
        PhysicalTimeSpec minLST;
        double waited_xact;
    };

    class PropagatedUpdate {
    public:

        PropagatedUpdate()
                : UpdatedItemAnchor(NULL),
                  UpdatedItemVersion(NULL) {
        }

        std::string SerializedRecord;
        std::string Key;
        std::string Value;
        PhysicalTimeSpec UT;
#ifdef MEASURE_VISIBILITY_LATENCY
        PhysicalTimeSpec CreationTime;
#endif
        int64_t LUT;

        PhysicalTimeSpec RST;

        int SrcPartition;
        int SrcReplica;
        void *UpdatedItemAnchor;
        void *UpdatedItemVersion;

#ifdef MINLSTKEY
        std::string minLSTKey;
#endif //MINLSTKEY
    };

    class Heartbeat {
    public:
        PhysicalTimeSpec PhysicalTime;
        int64_t LogicalTime;
    };

    class LocalUpdate {
    public:

        LocalUpdate()
                : UpdatedItemAnchor(NULL),
                  UpdatedItemVersion(NULL) {
        }

        void *UpdatedItemAnchor;
        void *UpdatedItemVersion;
        std::string SerializedRecord;
        bool delayed;
    };

    class UpdatedItemVersion {
    public:
        int64_t LUT;
        int SrcReplica;
    };

    typedef struct {

        long operator()(const UpdatedItemVersion &v) const {
            return (v.LUT << 4) + (v.SrcReplica);
        }
    } UpdatedVersionHash;

    typedef struct {

        long operator()(const UpdatedItemVersion &a, const UpdatedItemVersion &b) const {
            return (a.LUT == b.LUT) && (a.SrcReplica == b.SrcReplica);
        }
    } UpdatedVersionEq;

    typedef std::unordered_map<UpdatedItemVersion,
            PropagatedUpdate *,
            UpdatedVersionHash,
            UpdatedVersionEq> WaitingPropagatedUpdateMap;

    class ReplicationLatencyResult {
    public:
        PhysicalTimeSpec OverallLatency;
        PhysicalTimeSpec PropagationLatency;
        PhysicalTimeSpec VisibilityLatency;
        int SrcReplica;
    };

    enum class WorkloadType {
        READ_ALL_WRITE_LOCAL = 1,
        READ_WRITE_RATIO,
        WRITE_ROUND_ROBIN,
        RANDOM_READ_WRITE
    };

    class StalenessTimeMeasurement {
    public:
        StalenessTimeMeasurement() : sumTimes(0.0), count(0) {}

        double sumTimes;
        int count;
    };

    class VisibilityStatistics {
    public:
        double overall[12];
        double propagation[12];
        double visibility[12];

        VisibilityStatistics() {
            int i;
            for (i = 0; i < 12; i++) { overall[i] = -1; }
            for (i = 0; i < 12; i++) { propagation[i] = -1; }
            for (i = 0; i < 12; i++) { visibility[i] = -1; }
        }

    };

    class StalenessStatistics {
    public:
        double averageStalenessTime;
        double averageStalenessTimeTotal;
        double maxStalenessTime;
        double minStalenessTime;
        double averageUserPerceivedStalenessTime;
        double averageFirstVisibleItemVersionStalenessTime;
        double averageNumFresherVersionsInItemChain;
        double averageNumFresherVersionsInItemChainTotal;
        double medianStalenessTime;
        double _90PercentileStalenessTime;
        double _95PercentileStalenessTime;
        double _99PercentileStalenessTime;

        StalenessStatistics() : minStalenessTime(1000000.0),
                                maxStalenessTime(0.0),
                                averageStalenessTime(0.0),
                                averageFirstVisibleItemVersionStalenessTime(0.0),
                                averageNumFresherVersionsInItemChain(0.0),
                                averageNumFresherVersionsInItemChainTotal(0.0),
                                averageUserPerceivedStalenessTime(0.0),
                                averageStalenessTimeTotal(0.0),
                                medianStalenessTime(0.0),
                                _90PercentileStalenessTime(0.0),
                                _95PercentileStalenessTime(0.0),
                                _99PercentileStalenessTime(0.0) {}
    };

    class Statistic {
    public:
        std::string type;
        double sum;
        double average;
        double median;
        double min;
        double max;
        double _75Percentile;
        double _90Percentile;
        double _95Percentile;
        double _99Percentile;
        double variance;
        double standardDeviation;
        std::vector<double> values;
        mutable std::mutex valuesMutex;

        Statistic() : sum(0.0),
                      average(0.0),
                      median(0.0),
                      min(100000000.0),
                      max(0.0),
                      _75Percentile(0.0),
                      _90Percentile(0.0),
                      _95Percentile(0.0),
                      _99Percentile(0.0),
                      variance(0.0),
                      standardDeviation(0.0),
                      type("") {}

        Statistic(Statistic &&other) : sum(other.sum),
                                       average(other.average),
                                       median(other.median),
                                       min(other.min),
                                       max(other.max),
                                       _75Percentile(other._75Percentile),
                                       _90Percentile(other._90Percentile),
                                       _95Percentile(other._95Percentile),
                                       _99Percentile(other._99Percentile),
                                       variance(other.variance),
                                       standardDeviation(other.standardDeviation),
                                       type(other.type) {
            {
                std::lock_guard<std::mutex> lock(other.valuesMutex);
                for (int i = 0; i < values.size(); i++) {
                    values[i] = std::move(other.values[i]);
                }
            }
        }

        Statistic(Statistic &other) : sum(other.sum),
                                      average(other.average),
                                      median(other.median),
                                      min(other.min),
                                      max(other.max),
                                      _75Percentile(other._75Percentile),
                                      _90Percentile(other._90Percentile),
                                      _95Percentile(other._95Percentile),
                                      _99Percentile(other._99Percentile),
                                      variance(other.variance),
                                      standardDeviation(other.standardDeviation),
                                      type(other.type) {
            {
                std::lock_guard<std::mutex> lock(other.valuesMutex);
                for (int i = 0; i < values.size(); i++) {
                    values[i] = other.values[i];
                }
            }
        }

        Statistic &operator=(Statistic &&other) {
            sum = other.sum;
            average = other.average;
            median = other.median;
            min = other.min;
            max = other.max;
            _75Percentile = other._75Percentile;
            _90Percentile = other._90Percentile;
            _95Percentile = other._95Percentile;
            _99Percentile = other._99Percentile;
            variance = other.variance;
            standardDeviation = other.standardDeviation;
            type = other.type;

            {
                std::lock(valuesMutex, other.valuesMutex);
            }
            std::lock_guard<std::mutex> self_lock(valuesMutex, std::adopt_lock);
            std::lock_guard<std::mutex> other_lock(other.valuesMutex, std::adopt_lock);
            for (int i = 0; i < values.size(); i++) {
                values[i] = std::move(other.values[i]);
            }
            return *this;
        }

        Statistic &operator=(const Statistic &other) {
            sum = other.sum;
            average = other.average;
            median = other.median;
            min = other.min;
            max = other.max;
            _75Percentile = other._75Percentile;
            _90Percentile = other._90Percentile;
            _95Percentile = other._95Percentile;
            _99Percentile = other._99Percentile;
            variance = other.variance;
            standardDeviation = other.standardDeviation;
            type = other.type;

            {
                std::lock(valuesMutex, other.valuesMutex);
                std::lock_guard<std::mutex> self_lock(valuesMutex, std::adopt_lock);
                std::lock_guard<std::mutex> other_lock(other.valuesMutex, std::adopt_lock);
                for (int i = 0; i < values.size(); i++) {
                    values[i] = other.values[i];
                }
            }
            return *this;
        }


        void setToZero() {
            sum = 0;
            average = 0;
            median = 0;
            _75Percentile = 0;
            _99Percentile = 0;
            _95Percentile = 0;
            _90Percentile = 0;
            variance = 0;
            standardDeviation = 0;
            min = 0;
            max = 0;
        }

        std::string toString() {
            std::string
                    str = (boost::format("sum %s %.5lf\n"
                                         "average % s % .5lf\n"
                                         "median %s %.5lf\n"
                                         "min %s %.5lf\n"
                                         "max %s %.5lf\n"
                                         "75 percentile %s %.5lf\n"
                                         "90 percentile %s %.5lf\n"
                                         "95 percentile %s %.5lf\n"
                                         "99 percentile %s %.5lf\n")
                           % type % sum
                           % type % average
                           % type % median
                           % type % min
                           % type % max
                           % type % _75Percentile
                           % type % _90Percentile
                           % type % _95Percentile
                           % type % _99Percentile).str();

            return str;

        }

        void calculate();
    };


    void Statistic::calculate() {
        if (values.size() > 0) {
            sum = std::accumulate(values.begin(), values.end(), 0.0);
            average = sum / values.size();

            min = *(std::min_element(std::begin(values),
                                     std::end(values)));
            max = *(std::max_element(std::begin(values),
                                     std::end(values)));

            median = values.at((int) std::round(values.size() * 0.50));

            _75Percentile = values.at((int) std::round(values.size() * 0.75) - 1);

            _90Percentile = values.at((int) std::round(values.size() * 0.90) - 1);

            _95Percentile = values.at((int) std::round(values.size() * 0.95) - 1);

            _99Percentile = values.at((int) std::round(values.size() * 0.99) - 1);

            variance = 0;

            for (int i = 0; i < values.size(); i++) {
                variance += std::pow(average - values[i], 2);
            }

            variance = variance / values.size();
            standardDeviation = std::sqrt(variance);
        }

    }

    class OptVersionBlockStatistics : public Statistic {
    public:

        OptVersionBlockStatistics() : Statistic() {}

    };

/* >>>>>>>>>>>>>>>>>>> Read-Write Transactional Code <<<<<<<<<<<<<<<<<<<<<<< */
    class TxConsistencyMetadata {
    public:

        PhysicalTimeSpec UST; /* global remote stable time */
        PhysicalTimeSpec CT; /* latest commit time */


        uint64_t txId; /* transaction id */ //std::atomic<int>
    };

    class Item;

    class TxClientMetadata : public TxConsistencyMetadata {
    public:
        std::unordered_map<std::string, std::string> ReadSet;  /* <key, value> */
        std::unordered_map<std::string, std::string> WriteSet; /* <key, value> */
        std::unordered_map<std::string, std::pair<std::string, PhysicalTimeSpec>> WriteCache; /* <key, value> */

#ifdef NO_START_TX
        bool txIdAssigned;
#endif //NO_START_TX

    };


    class ServerMetadata {
    public:
        std::mutex gstMutex;
        PhysicalTimeSpec GST; /* global stable time */


    };


    class TxContex {
    public:
        PhysicalTimeSpec UST; /* remote stable time */
        PhysicalTimeSpec HT; /* clock update time for the prepare request */


        PhysicalTimeSpec startTimePreparedSet;
        PhysicalTimeSpec endTimePreparedSet;
        PhysicalTimeSpec startTimeCommitedSet;
        PhysicalTimeSpec endTimeCommitedSet;

        double waited_xact;
        double timeWaitedOnPrepareXact;


        TxContex() : UST(0, 0), HT(0, 0) {
            waited_xact = 0.0;
            timeWaitedOnPrepareXact = 0.0;
        }

        TxContex(PhysicalTimeSpec grst) {

            this->UST = grst;
            waited_xact = 0.0;
            timeWaitedOnPrepareXact = 0.0;
        }

        TxContex(const TxContex &ctx) {


            this->UST = ctx.UST;
            this->HT = ctx.HT;


            waited_xact = ctx.waited_xact;
            timeWaitedOnPrepareXact = ctx.timeWaitedOnPrepareXact;

            startTimePreparedSet = ctx.startTimePreparedSet;
            endTimePreparedSet = ctx.endTimePreparedSet;
            startTimeCommitedSet = ctx.startTimeCommitedSet;
            endTimeCommitedSet = ctx.endTimeCommitedSet;
        }


        TxContex &operator=(const TxContex &ctx) {

            this->UST = ctx.UST;
            this->HT = ctx.HT;


            waited_xact = ctx.waited_xact;
            timeWaitedOnPrepareXact = ctx.timeWaitedOnPrepareXact;

            startTimePreparedSet = ctx.startTimePreparedSet;
            endTimePreparedSet = ctx.endTimePreparedSet;
            startTimeCommitedSet = ctx.startTimeCommitedSet;
            endTimeCommitedSet = ctx.endTimeCommitedSet;

        }


    };

    class Item {
        std::string key;
        std::string value;
        PhysicalTimeSpec updateTime;
        int srcReplica;
    public:

        Item() {
            key = "";
            value = "";
            updateTime.setToZero();
            srcReplica = 0;
        }

        Item(const std::string &key, const std::string &value, const PhysicalTimeSpec &ut, int sr) : key(key),
                                                                                                     value(value),
                                                                                                     srcReplica(sr) {
            this->updateTime = ut;
        }

        Item(const std::string &key) : key(key) {
            this->value = "";
            this->updateTime.setToZero();
            this->srcReplica = 0;
        }


        Item(const Item &rhs) {
            key = rhs.key;
            value = rhs.value;
            updateTime = rhs.updateTime;
            srcReplica = rhs.srcReplica;
        }

        Item &operator=(const Item &rhs) {
            key = rhs.key;
            value = rhs.value;
            updateTime = rhs.updateTime;
            srcReplica = rhs.srcReplica;
        }

        bool operator==(const Item &rhs) const {
            return key == rhs.key &&
                   value == rhs.value &&
                   updateTime == rhs.updateTime &&
                   srcReplica == rhs.srcReplica;
        }


        bool operator!=(const Item &rhs) const {
            return !(rhs == *this);
        }

        const std::string &getKey() const {
            return key;
        }

        void setKey(const std::string &key) {
            Item::key = key;
        }

        const std::string &getValue() const {
            return value;
        }

        void setValue(const std::string &value) {
            Item::value = value;
        }

        const PhysicalTimeSpec &getUpdateTime() const {
            return updateTime;
        }

        void setUpdateTime(const PhysicalTimeSpec &updateTime) {
            Item::updateTime = updateTime;
        }

        int getSrcReplica() const {
            return srcReplica;
        }

        void setSrcReplica(int srcReplica) {
            Item::srcReplica = srcReplica;
        }

    };

} // namespace scc

#endif
