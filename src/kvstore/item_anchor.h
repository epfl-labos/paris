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



#ifndef SCC_KVSTORE_ITEM_ANCHOR_
#define SCC_KVSTORE_ITEM_ANCHOR_

#include "kvstore/item_version.h"
#include "common/sys_config.h"
#include "common/sys_logger.h"
#include "common/sys_stats.h"
#include "common/utils.h"
#include "common/types.h"
#include <thread>
#include <list>
#include <utility>
#include <string>
#include <boost/format.hpp>
#include <iostream>

namespace scc {

    class ItemAnchor {
    public:
        ItemAnchor(std::string itemKey, int numReplicas, int replicaId,
                   std::unordered_map<int, int> _ridToIndex);

        ~ItemAnchor();

        void InsertVersion(ItemVersion *version);

        static int _replicaId;


        ItemVersion *LatestSnapshotVersion(const PhysicalTimeSpec &snapshotLDT, const PhysicalTimeSpec &snapshotRST);
        ItemVersion *LatestSnapshotVersion(const PhysicalTimeSpec &snapshotST);
        ItemVersion *LatestVersion();


        void MarkLocalUpdatePersisted(ItemVersion *version);

        std::string ShowItemVersions();

        std::string _itemKey;
    private:

        std::mutex _itemMutex;
        std::vector<ItemVersion *> _latestVersion;
        std::unordered_map<int, int> _ridToVersionIndex;
        ItemVersion *_lastAddedVersion;


        void _calculateUserPercievedStalenessTime(ItemVersion *firstNotVisibleItem, int replicaId,
                                                  PhysicalTimeSpec timeGet, std::string stalenessStr);

        bool isHot(std::string basic_string);

        inline ItemVersion *_getLatestItem();

        inline bool _isGreaterOrEqual(std::vector<PhysicalTimeSpec> v1, std::vector<PhysicalTimeSpec> v2);

        inline PhysicalTimeSpec _maxElem(std::vector<PhysicalTimeSpec> v);

        int getNextItemIndex(const std::vector<ItemVersion *> &current);

        bool isItemVisible(ItemVersion *next, const PhysicalTimeSpec &snapshotLDT, const PhysicalTimeSpec &snapshotRST);


        bool areItemsRemoteDepLessThanSnapshotVector(const ItemVersion *next,
                                                     const std::vector<PhysicalTimeSpec> &snapshotVector) const;
    };

} // namespace scc

#endif
