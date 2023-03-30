/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright Futurewei Technologies Inc, 2020
 * Copyright Justin Funston 2023
 */

#pragma once

#include <deque>
#include <unordered_map>
#include <array>
#include <functional>
#include <string>

#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/future.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/core/sstring.hh>

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <sys/socket.h>

namespace seastar { namespace rdma { class EndPoint; }}

namespace std {

template <>
struct hash<seastar::rdma::EndPoint> {
    std::size_t operator()(const seastar::rdma::EndPoint&) const;
};

}

namespace seastar {
namespace rdma {

class RDMAStack;
class RDMAListener;
typedef temporary_buffer<char> Buffer;

struct RecvWRData {
    static constexpr int maxWR = 256;
    struct ibv_recv_wr RecvRequests[maxWR];
    struct ibv_sge Segments[maxWR];
};

struct SendWRData {
    static constexpr int maxWR = 32;
    static constexpr int signalThreshold = 16;
    static_assert(maxWR % signalThreshold == 0);
    struct ibv_send_wr SendRequests[maxWR];
    struct ibv_sge Segments[maxWR];
    uint32_t postedIdx = 0;
    uint32_t postedCount = 0;

    SendWRData() = default;
    SendWRData(SendWRData&&) = default;
    SendWRData& operator=(SendWRData&&) = default;
};

struct EndPoint {
   struct sockaddr_in addr;
    EndPoint(struct sockaddr a) :
        addr(a) {}

    EndPoint() = default;
    bool operator==(const EndPoint& rhs) const = default;
};

class RDMAConnection : public weakly_referencable<RDMAConnection> {
public:
    future<Buffer> recv();
    void send(std::vector<Buffer>&& buf);

    RDMAConnection(RDMAStack* stack, EndPoint remote) :
        stack(stack), remote(remote) {}
    ~RDMAConnection() noexcept;
    RDMAConnection(RDMAConnection&&) = delete;
    RDMAConnection& operator=(RDMAConnection&&) = delete;
    bool closed() {
        return errorState;
    }
    future<> close() {
        if (errorState) {
            return make_ready_future<>();
        }

        sendCloseSignal();
        closePromiseActive = true;
        closePromise = promise<>();
        return closePromise.get_future();
    }
    const EndPoint& getAddr() const {
        return remote;
    }
private:
    bool isReady = false;
    bool errorState = false;
    std::deque<Buffer> recvQueue;

    // Messages below this size will be copied into packed RDMA sends
    // if possible
    static constexpr int messagePackThreshold = 200;
    // The number of bytes allocated but not used in the last temporary_buffer
    // of the sendQueue. Used to pack new messages into fewer send requests
    size_t sendQueueTailBytesLeft = 0;
    std::deque<Buffer> sendQueue;

    SendWRData sendWRs;
    std::array<Buffer, SendWRData::maxWR> outstandingBuffers;
    template <class VecType>
    void processSends(VecType& queue);
    void incomingMessage(char* data, uint32_t size);
    void sendCloseSignal();

    struct ibv_qp* QP = nullptr;
    RDMAStack* stack = nullptr;
    EndPoint remote;

    bool closePromiseActive = false;
    promise<> closePromise;

    bool recvPromiseActive = false;
    promise<Buffer> recvPromise;

    future<> makeQP();
    void makeHandshakeRequest();
    future<> completeHandshake(uint32_t remoteQP);
    void processHandshakeRequest(uint32_t remoteQP, uint32_t responseId);

    void shutdownConnection();

    RDMAConnection() = delete;

    friend class RDMAStack;
};

class RDMAStack : public weakly_referencable<RDMAStack> {
public:
    EndPoint localEndpoint;

    future<std::unique_ptr<RDMAConnection>> accept();
    std::unique_ptr<RDMAConnection> connect(const EndPoint& remote);

    static std::unique_ptr<RDMAStack> makeRDMAStack(void* memRegion, size_t memRegionSize, uint8_t gid_index);
    RDMAStack() = default;
    ~RDMAStack() noexcept;

    bool poller();
    RDMAListener listen();

    static constexpr uint32_t RCDataSize = 8192;
private:
    struct rdma_cm_event* connection_queue = nullptr;
    struct rdma_cm_id* listen_id = nullptr;

    std::optional<reactor::poller> RDMAPoller;
    struct ibv_pd* protectionDomain = nullptr;
    struct ibv_mr* memRegionHandle = nullptr;

    bool pollConnectionQueue();
    static void processCompletedSRs(std::array<Buffer, SendWRData::maxWR>& buffers, SendWRData& WRData, uint64_t signaledID);
    static constexpr int pollBatchSize = 16;

    static constexpr int maxExpectedConnections = 1024;
    static constexpr int RCCQSize = RecvWRData::maxWR +
            (maxExpectedConnections * (SendWRData::maxWR / SendWRData::signalThreshold));
    RecvWRData RCQPRRs;
    struct ibv_cq* RCCQ = nullptr;
    struct ibv_srq* SRQ = nullptr;
    std::unordered_map<uint32_t, weak_ptr<RDMAConnection>> RCLookup;
    bool processRCCQ();
    int RCConnectionCount = 0;

    void registerPoller();
    void registerMetrics();

    bool acceptPromiseActive = false;
    promise<std::unique_ptr<RDMAConnection>> acceptPromise;
    std::deque<std::unique_ptr<RDMAConnection>> acceptQueue;

    // For metrics
    uint64_t totalRecv=0;
    uint64_t totalSend=0;
    uint64_t sendQueueSize=0;
    uint64_t sendQueueSum=0;
    uint64_t sendQueueCount=0;
    uint64_t sendBatchSum=0;
    uint64_t sendBatchCount=0;
    uint64_t recvBatchSum=0;
    uint64_t recvBatchCount=0;
    seastar::metrics::metric_groups metricGroup;

    friend class RDMAConnection;
    friend class RDMAListener;
};

class RDMAListener {
public:
    RDMAListener():_rstack(0){}
    RDMAListener(RDMAStack* rstack):_rstack(rstack) {}
    ~RDMAListener() {}
    future<std::unique_ptr<RDMAConnection>> accept() {
        assert(_rstack);
        return _rstack->accept();
    }
    future<> close() {
        if (_rstack && _rstack->acceptPromiseActive) {
            _rstack->acceptPromiseActive = false;
            _rstack->acceptPromise.set_exception(std::runtime_error("Shutting down accept promise on listener close"));
        }
        return make_ready_future<>();
    }
private:
    RDMAStack* _rstack;
}; // class RDMAListener

} // namespace rdma
} // namespace seastar
