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



#ifndef SCC_RPC_ABSTRACT_RPC_CLIENT_H
#define SCC_RPC_ABSTRACT_RPC_CLIENT_H

#include "rpc/message_channel.h"
#include "rpc/rpc_id.h"
#include <string>
#include <thread>
#include <atomic>

namespace scc {

class AbstractRPCClient {
public:

  AbstractRPCClient()
    : _msgIdCounter(0) {
  }

protected:
  std::atomic<int64_t> _msgIdCounter;

  int64_t GetMessageId() {
    return _msgIdCounter.fetch_add(1);
  }
};

}

#endif
