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



#ifndef SCC_KVSTORE_ITEM_VERSION_H_
#define SCC_KVSTORE_ITEM_VERSION_H_

#include "common/utils.h"
#include <string>
#include <boost/format.hpp>

namespace scc {

    class ItemVersion {
    public:
        std::string Value;
        int64_t LUT; // logical update time
        PhysicalTimeSpec UT; // update time which is differently handled according to the protocol type
        int SrcReplica;
        int SrcPartition;
        bool Persisted;
        ItemVersion *Next;
        bool blocked;
        // visibility measurement
#ifdef MEASURE_VISIBILITY_LATENCY
        PhysicalTimeSpec CreationTime;
#endif
        std::string Key;
        PhysicalTimeSpec RIT; //replica install time

        PhysicalTimeSpec DST; // dependency stable time


    public:
        ItemVersion() {
            Next = NULL;
        }

        ~ItemVersion() {
            delete Next;
        }

        ItemVersion(const std::string &value) {
            Value = value;
            Next = NULL;
        }

        ItemVersion(const ItemVersion &iv) {
        std::cout<<"\nIn constructor=\n";
            this->Value = iv.Value;
            this->LUT = iv.LUT;
            this->UT = iv.UT;
            this->SrcPartition = iv.SrcPartition;
            this->SrcReplica = iv.SrcReplica;
            this->Persisted = iv.Persisted;
            this->Key = iv.Key;
            this->RIT = iv.RIT;

#ifdef MEASURE_VISIBILITY_LATENCY
            this->CreationTime =iv.CreationTime;
#endif
            this->DST =iv.DST;


            if (iv.Next != NULL)
                this->Next = new ItemVersion(*iv.Next);
            else
                this->Next = NULL;
        }

        ItemVersion &operator=(const ItemVersion &iv) {
            std::cout<<"\nIn operator=\n";

            this->Value = iv.Value;
            this->LUT = iv.LUT;
            this->UT = iv.UT;
            this->SrcPartition = iv.SrcPartition;
            this->SrcReplica = iv.SrcReplica;
            this->Persisted = iv.Persisted;
            this->Key = iv.Key;
            this->RIT = iv.RIT;
#ifdef MEASURE_VISIBILITY_LATENCY
            this->CreationTime =iv.CreationTime;
#endif
            this->DST =iv.DST;

            std::cout<<"\n2\n";
            if (iv.Next != NULL)
                this->Next = new ItemVersion(*iv.Next);
            else
                this->Next = NULL;
        }


        std::string ShowItemVersion();
    };

    std::string ItemVersion::ShowItemVersion() {
        std::string versionStr;

        versionStr = (boost::format("(lut %d, src replica %d, persisted %s)")
                      % LUT
                      % SrcReplica
                      % (Persisted ? "true" : "false")
        ).str();

        return versionStr;
    }

    bool operator==(const ItemVersion &a, const ItemVersion &b) {
        return (a.UT == b.UT) &&
               (a.SrcReplica == b.SrcReplica);
    }

    bool operator>(const ItemVersion &a, const ItemVersion &b) {
        if (a.UT > b.UT) {
            return true;
        } else if (a.UT == b.UT) {
            if (a.SrcReplica > b.SrcReplica) {
                return true;
            }
        }

        return false;
    }

    bool operator<=(const ItemVersion &a, const ItemVersion &b) {
        return !(a > b);
    }

    bool operator<(const ItemVersion &a, const ItemVersion &b) {
        if (a.UT < b.UT) {
            return true;
        } else if (a.UT == b.UT) {
            if (a.SrcReplica < b.SrcReplica) {
                return true;
            }
        }

        return false;
    }

    bool operator>=(const ItemVersion &a, const ItemVersion &b) {
        return !(a < b);
    }

} // namespace scc

#endif
