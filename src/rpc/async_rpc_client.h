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



#ifndef SCC_RPC_ASYNC_RPC_CLIENT_H
#define SCC_RPC_ASYNC_RPC_CLIENT_H

#include "rpc/abstract_rpc_client.h"
#include "rpc/message_channel.h"
#include "messages/rpc_messages.pb.h"
#include "common/wait_handle.h"
#include <vector>
#include <unordered_map>
#include <thread>
#include <memory>
#include <pthread.h>
#include <atomic>

namespace scc {

class AsyncState {
public:
  WaitHandle FinishEvent;
  std::string Results;
};

class AsyncRPCClient : public AbstractRPCClient {
public:
  AsyncRPCClient(std::string host, int port, int numChannels);
  ~AsyncRPCClient();
  void Call(RPCMethod rid, std::string& args, AsyncState& state);
    void CallWithNoState(RPCMethod rid, std::string& args);
private:
  typedef std::unordered_map<int64_t, AsyncState*> PendingCallMap;
  typedef std::shared_ptr<PendingCallMap> PendingCallMapPtr;
  typedef std::shared_ptr<std::mutex> MutexPtr;

  // initialization synchronization
  std::vector<WaitHandle*> _replyHandlingThreadReadyEvents;
  WaitHandle _allReplyHandlingThreadsReady;
  int _numChannels;
  std::vector<MessageChannelPtr> _msgChannels;
  std::vector<PendingCallMapPtr> _pendingCallSets;
  std::vector<MutexPtr> _pendingCallSetMutexes;
  void HandleReplyMessage(int channelId);
};

typedef std::shared_ptr<AsyncRPCClient> AsyncRPCClientPtr;

} // namespace scc

#endif
