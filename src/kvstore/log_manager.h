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



#ifndef SCC_KVSTORE_LOG_MANAGER_H_
#define SCC_KVSTORE_LOG_MANAGER_H_

#include "common/wait_handle.h"
#include "common/types.h"
#include <string>
#include <queue>
#include <list>
#include <thread>
#include <atomic>
#include <set>

namespace scc {

    class LogManager {
    public:
        static LogManager *Instance();

        ~LogManager();

        void Initialize(int numReplicas);

        // persist local update
        void AppendLog(LocalUpdate *, WaitHandle *persistedEvent);

        // replicate propagated update
        void AppendReplicatedUpdate(PropagatedUpdate *update);

    public:

        // local updates
        WaitHandle ReplicationWaitHandle;
        std::vector<LocalUpdate *> ToPropagateLocalUpdateQueue;
        std::vector<LocalUpdate *> ToPropagateLocalUpdateQueue2;
        std::vector<LocalUpdate *> *ToPropagateLocalUpdateQueuePtr;

        std::mutex ToPropagateLocalUpdateQueueMutex;

        // propagated updates
        std::vector<std::mutex *> PersistedPropagatedUpdateQueueMutexes;
        std::vector<std::vector<PropagatedUpdate *>> PersistedPropagatedUpdateQueues;
        std::vector<WaitHandle *> PersistedPropagatedUpdateEvents;

    private:
        LogManager();

        int _logfd;

        std::vector<LocalUpdate *> _localUpdateQueue;
        std::vector<std::vector<PropagatedUpdate *>> _replicatedUpdateQueues;
        std::mutex _reqQueueMutex;
        WaitHandle _reqAvailable;
        std::vector<WaitHandle *> _opPersistedEventQueue;

        // used by Durability::Memory
        std::list<char *> _inMemoryLog;

        std::thread *_workerThread;

        void worker();

    public:
        std::atomic<int64_t> NumReplicatedBytes;

        std::vector<LocalUpdate *> *GetCurrUpdates(void);
    };

} // namespace scc

#endif
