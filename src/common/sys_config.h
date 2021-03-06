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

#ifndef SCC_COMMON_SYS_CONFIG_H
#define SCC_COMMON_SYS_CONFIG_H

#include "common/types.h"
#include <string>

namespace scc {

    class SysConfig {
    public:
        static ConsistencyType Consistency;
        static bool OptimisticMode;
        static GSTDerivationType GSTDerivationMode;
        static int NumItemIndexTables;
        static std::string PreallocatedOpLogFile;
        static std::string OpLogFilePrefix;
        static DurabilityType DurabilityOption;
        static std::string SysLogFilePrefix;
        static std::string SysDebugFilePrefix;
        static bool SysLogEchoToConsole;
        static bool SysDebugEchoToConsole;
        static bool Debugging;
        static int NumChannelsPartition;
        static int UpdatePropagationBatchTime; // microseconds

        static int ReplicationHeartbeatInterval; // microseconds

        static int NumChildrenOfTreeNode;
        static int GSTComputationInterval; // microseconds
        static int RSTComputationInterval; // microseconds

        static bool MeasureVisibilityLatency;
        static int LatencySampleInterval;
        static bool WaitConditionInOptimisticSetOperation;

        static int GetSpinTime;
        static int OpttxDelta;

        static int FreshnessParameter;
    };

} // namespace scc

#endif
