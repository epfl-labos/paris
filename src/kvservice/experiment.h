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



#ifndef SCC_KVSERVICE_EXPERIMENT_H
#define SCC_KVSERVICE_EXPERIMENT_H

#include "kvservice/public_kv_client.h"
#include "groupservice/group_client.h"
#include "common/types.h"
#include "common/utils.h"
#include "common/generators.h"
#include "common/wait_handle.h"
#include "common/sys_config.h"
#include <sys/types.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>
#include <unistd.h>
#include <string>
#include <numeric>
#include <thread>
#include <chrono>
#include <algorithm>
#include <iostream>
#include <stdexcept>

namespace scc {

    typedef struct {
        int ThreadId;
        std::string workload;
        std::unordered_map<int, std::string> ServerNames;
        std::unordered_map<int, int> ServerPorts;
        std::vector<PublicTxClient *> Clients;
        PublicTxClient *Client;
        int LocalPartitionId;
        int NumPartitions;
        int NumDataCenters;
        int ReplicationFactor;
        int TotalNumItems;
        int NumValueBytes;
        int locality;

        bool ReservoirSampling;

        int ReservoirSamplingLimit;
        atomic<bool> *stopOperation;

        int servingReplica;
        int servingPartition;
        std::string RequestsDistribution;

        int ReadRatio;
        int WriteRatio;
        int TxRatio;
        int LOTxRatio;
        int LRTxRatio;

        Generator<uint64_t> *generator;
        WaitHandle OpReadyEvent;
        WaitHandle OpStartEvent;
        WaitHandle OpEndEvent;

        int NumTxsPerThread;
        int NumLocalTxsPerThread;
        int NumMixedTxsPerThread;


        int NumStartOpsPerTx;
        int NumReadOpsPerTx;
        int NumWriteOpsPerTx;
        int NumCommitOpsPerTx;

        int TotalNumStartOpsPerThread;
        int TotalNumReadOpsPerThread;
        int TotalNumLocalReadOpsPerThread;
        int TotalNumMixedReadOpsPerThread;
        int TotalNumWriteOpsPerThread;
        int TotalNumCommitOpsPerThread;
        int TotalNumTxOpsPerThread;
        int TotalNumLocalTxOpsPerThread;
        int TotalNumMixedTxOpsPerThread;

        std::vector<double> Latencies;
        std::vector<double> OpStartLatencies;
        std::vector<double> OpReadLatencies;
        std::vector<double> OpLocalReadLatencies;
        std::vector<double> OpMixedReadLatencies;
        std::vector<double> OpWriteLatencies;
        std::vector<double> OpCommitLatencies;
        std::vector<double> OpTxLatencies;
        std::vector<double> OpLocalTxLatencies;
        std::vector<double> OpMixedTxLatencies;

        double StartLatenciesSum;
        double ReadLatenciesSum;
        double LocalReadsLatenciesSum;
        double MixedReadsLatenciesSum;
        double WriteLatenciesSum;
        double CommitLatenciesSum;
        double TxLatenciesSum;
        double LocalTxLatenciesSum;
        double MixedTxLatenciesSum;

        double MinStartLatency;
        double MaxStartLatency;

        double MinReadLatency;
        double MaxReadLatency;

        double MinLocalReadLatency;
        double MaxLocalReadLatency;

        double MinMixedReadLatency;
        double MaxMixedReadLatency;

        double MinWriteLatency;
        double MaxWriteLatency;

        double MinTxLatency;
        double MaxTxLatency;

        double MinLocalTxLatency;
        double MaxLocalTxLatency;

        double MinMixedTxLatency;
        double MaxMixedTxLatency;

        double MinCommitLatency;
        double MaxCommitLatency;

        int NumTxReadItems;
        int NumTxWriteItems;

        int numHotBlockKeys;
        int clientResetNumber;
        bool enableClientReset;

        int TotalNumReadItems;

        double warmUpDuration;

        int64_t numItemsReadFromClientWriteSet;
        int64_t numItemsReadFromClientReadSet;
        int64_t numItemsReadFromClientWriteCache;
        int64_t numItemsReadFromStore;
        int64_t totalNumClientReadItems;
        WaitHandle *allReady;

        void initialize() {
            ThreadId = 0;
            workload = "";

            LocalPartitionId = 0;
            NumPartitions = 0;
            NumDataCenters = 0;
            ReplicationFactor = 0;
            TotalNumItems = 0;
            NumValueBytes = 0;
            locality = 0;

            ReservoirSampling = false;

            ReservoirSamplingLimit = 0;

            servingReplica = 0;
            servingPartition = 0;
            RequestsDistribution = "";

            ReadRatio = 0;
            WriteRatio = 0;
            TxRatio = 0;
            LOTxRatio = 0;
            LRTxRatio = 0;

            NumTxsPerThread = 0;

            NumStartOpsPerTx = 0;
            NumReadOpsPerTx = 0;
            NumWriteOpsPerTx = 0;
            NumCommitOpsPerTx = 0;

            TotalNumStartOpsPerThread = 0;
            TotalNumReadOpsPerThread = 0;
            TotalNumWriteOpsPerThread = 0;
            TotalNumCommitOpsPerThread = 0;
            TotalNumTxOpsPerThread = 0;
            TotalNumLocalReadOpsPerThread = 0;
            TotalNumMixedReadOpsPerThread = 0;

            StartLatenciesSum = 0;
            ReadLatenciesSum = 0;
            LocalReadsLatenciesSum = 0;
            MixedReadsLatenciesSum = 0;
            WriteLatenciesSum = 0;
            CommitLatenciesSum = 0;
            TxLatenciesSum = 0;

            MinStartLatency = 0;
            MaxStartLatency = 0;

            MinReadLatency = 0;
            MaxReadLatency = 0;

            MinLocalReadLatency = 0;
            MaxLocalReadLatency = 0;

            MinMixedReadLatency = 0;
            MaxMixedReadLatency = 0;

            MinWriteLatency = 0;
            MaxWriteLatency = 0;

            MinTxLatency = 0;
            MaxTxLatency = 0;

            MinCommitLatency = 0;
            MaxCommitLatency = 0;

            NumTxReadItems = 0;
            NumTxWriteItems = 0;

            numHotBlockKeys = 0;
            clientResetNumber = 0;
            enableClientReset = 0;

            TotalNumReadItems = 0;

            warmUpDuration = 0;

            numItemsReadFromClientWriteSet = 0;
            numItemsReadFromClientReadSet = 0;
            numItemsReadFromClientWriteCache = 0;
            numItemsReadFromStore = 0;
            totalNumClientReadItems = 0;

        }


    } ThreadArg;

    typedef struct {
        int NumPartitions;
        int NumDataCenters;
        int ReplicationFactor;
        int NumThreadsPerReplica;
        int NumOpsPerThread;
        int NumHotBlockedKeys;
        int NumStartOpsPerTx;
        int NumReadOpsPerTx;
        int NumWriteOpsPerTx;
        int NumCommitOpsPerTx;
        double OpThroughput;
        double TxThroughput;
        int64_t TotalOps;
        int64_t TotalTxOps;
        int64_t TotalLocalTxOps;
        int64_t TotalMixedTxOps;
        int64_t TotalStartOps;
        int64_t TotalReadOps;
        int64_t TotalLocalReadOps;
        int64_t TotalMixedReadOps;
        int64_t TotalWriteOps;
        int64_t TotalCommitOps;
        int64_t TotalExpDuration;
        int64_t TotalNumReadItems;
        Statistic OpStartLatencyStats;
        Statistic OpReadLatencyStats;
        Statistic OpLocalReadLatencyStats;
        Statistic OpMixedReadLatencyStats;
        Statistic OpWriteLatencyStats;
        Statistic OpCommitLatencyStats;
        Statistic OpTxLatencyStats;
        Statistic OpLocalTxLatencyStats;
        Statistic OpMixedTxLatencyStats;
        std::vector<double> startLatencies;
        std::vector<double> readLatencies;
        std::vector<double> localReadLatencies;
        std::vector<double> mixedReadLatencies;
        std::vector<double> writeLatencies;
        std::vector<double> commitLatencies;
        std::vector<double> txLatencies;
        std::vector<double> localTxLatencies;
        std::vector<double> mixedTxLatencies;

        int64_t numItemsReadFromClientWriteSet;
        int64_t numItemsReadFromClientReadSet;
        int64_t numItemsReadFromClientWriteCache;
        int64_t numItemsReadFromStore;
        int64_t totalNumClientReadItems;
        int missingThreads;

    } Results;

    class Experiment {
    public:
        std::atomic<bool> stopOperation;

        Experiment(char *argv[]);

        void runExperiment();

        Configuration config;
    private:
        std::vector<std::vector<DBPartition>> allPartitions;
        std::vector<ThreadArg *> threadArgs;
        std::vector<std::thread *> threads;
        std::vector<WaitHandle *> opReadyEvents;
        std::vector<WaitHandle *> opEndEvents;
        WaitHandle *allReady;

        void initializeThreadRequestDistribution(ThreadArg *t);

        void buildThreadArguments();

        void launchBenchmarkingClientThreads();

        void calculatePerformanceMeasures(double duration);

        void writeResultsCSV(FILE *&stream, Results res);

        void freeResources();

        void printExperimentParameters();

        void writeLatencyDistribution(std::string fileName, vector<double> vector);

        void calculateVariation_StandardDeviation(Statistic &stat, vector<double> vect);

        void calculateMedian_Percentiles(vector<double> vect, Statistic &stat);

        int collectThreadsMeasurements();

        void setAvgIfCountNotZero(int64_t count, double &avg);
    };

} // namespace scc

#endif
