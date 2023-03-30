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
 */

#include <iostream>
#include <algorithm>
#include <iterator>
#include <memory>

#include "Log.h"
#include <seastar/net/rdma.hh>
#include <seastar/core/metrics_registration.hh> // metrics
#include <seastar/core/metrics.hh>
#include "core/thread_pool.hh"

#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <cstring>
#include <cerrno>

std::size_t std::hash<seastar::rdma::EndPoint>::operator()(const seastar::rdma::EndPoint& endpoint) const {
    return std::hash<unsigned short>{}(endpoint.addr.sin_port) ^ std::hash<unsigned long>{}(endpoint.addr.sin_addr.s_addr);
}

namespace seastar {

namespace rdma {

future<> RDMAConnection::makeQP() {
    // A Reliable Connected (RC) QP has five states:
    // RESET: the state of a newly created QP
    // INIT: Receive requests can be posted, but no receives or
    //       sends will occur
    // RTR: Ready-to-receive. Receives can be completed
    // RTS: Ready-to-send. Sends can be posted and completed
    // ERR: Error state.
    // To use a RC QP, the states must be transitioned in order:
    // RESET->INIT->RTR->RTS using ibv_modify_qp without skipping states
    // see the ibv_modify_qp documentation for more info

    struct ibv_qp_init_attr init_attr = {
        .qp_context = nullptr,
        .send_cq = stack->RCCQ,
        .recv_cq = stack->RCCQ,
        .srq = stack->SRQ,
        .cap = {
            .max_send_wr = SendWRData::maxWR,
            .max_recv_wr = RecvWRData::maxWR,
            .max_send_sge = 1,
            .max_recv_sge = 1,
            .max_inline_data = 0},
        .qp_type = IBV_QPT_RC,
        .sq_sig_all = 0
    };

    return engine()._thread_pool->submit<struct ibv_qp*>([init_attr, PD=stack->protectionDomain]() mutable {
        // Create QP
        // For all ll ibv_* calls with attr (attribute) parameters, the attr is passed by pointer because
        // it is a C interface, but the struct does not need to live beyond the ibv_ call.
        struct ibv_qp* QP = ibv_create_qp(PD, &init_attr);
        if (!QP) {
            K2ERROR("Failed to create RC QP: " << strerror(errno));
            return QP;
        }

        // Transition QP to INIT state
        struct ibv_qp_attr attr = {};
        attr.qp_state = IBV_QPS_INIT;
        attr.port_num = 1;
        if (int err = ibv_modify_qp(QP, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                                               IBV_QP_PORT | IBV_QP_ACCESS_FLAGS)) {
            K2ERROR("failed to transition RC QP into init state: " << strerror(err));
            ibv_destroy_qp(QP);
            QP = nullptr;
            return QP;
        }

        return QP;
    }).
    then([conn=weak_from_this()] (struct ibv_qp* newQP) {
        if (!conn) {
            if (newQP) {
                ibv_destroy_qp(newQP);
            }
            return;
        }

        if (!newQP) {
            conn->shutdownConnection();
            return;
        }

        conn->QP = newQP;
        if (conn->stack->RCConnectionCount+1 > RDMAStack::maxExpectedConnections) {
            K2WARN("CQ overrun possible");
        }
        conn->stack->RCLookup[conn->QP->qp_num] = conn->weak_from_this();
    }).
    handle_exception([conn=weak_from_this()] (auto e) {
        if (!conn) {
            return;
        }

        conn->shutdownConnection();
    });
}

// This method sends a zero-byte message to the destination, which
// indicates a graceful close. It also makes the send signaled so that
// we can know when the send queue is flushed
void RDMAConnection::sendCloseSignal() {
    if (!QP || errorState) {
        return;
    }

    int idx = (sendWRs.postedIdx + sendWRs.postedCount) % SendWRData::maxWR;
    struct ibv_send_wr* SR = &(sendWRs.SendRequests[idx]);

    SR->send_flags = IBV_SEND_SIGNALED;
    SR->next = nullptr;
    SR->sg_list = nullptr;
    SR->num_sge = 0;

    struct ibv_send_wr* badSR;
    if (int err = ibv_post_send(QP, SR, &badSR)) {
        K2ASSERT(false, "Failed to post send on RC QP: " << strerror(err));
        shutdownConnection();
        return;
    }

    // Even though this should be the last SR on this connection, we will
    // reset num_sge to the value the rest of the code expects
    SR->num_sge = 1;

    sendWRs.postedCount++;
}

template <class VecType>
void RDMAConnection::processSends(VecType& queue) {
    if (!QP) {
        // This case means an error occured and we shutdown the connection
        queue.clear();
        sendQueue.clear();
        sendWRs.postedCount = 0;
    }

    if (closePromiseActive && sendQueue.size() == 0 && sendWRs.postedCount == 0
                                                    && queue.size() == 0) {
        shutdownConnection();
        closePromiseActive = false;
        closePromise.set_value();
        return;
    }
    else if (queue.size() == 0 || sendWRs.postedCount == SendWRData::maxWR) {
        return;
    }

    int toProcess = std::min((int)queue.size(), (int)(SendWRData::maxWR - sendWRs.postedCount));
    int idx = (sendWRs.postedIdx + sendWRs.postedCount) % SendWRData::maxWR;
    int baseIdx = idx;
    struct ibv_send_wr* firstSR = &(sendWRs.SendRequests[idx]);

    for(int i=0; i < toProcess; ++i, idx = (baseIdx + i) % SendWRData::maxWR) {
        Buffer& sendData = queue[i];
        if (sendQueueTailBytesLeft && std::addressof(sendData) == std::addressof(sendQueue.back())) {
            sendData.trim(RDMAStack::RCDataSize - sendQueueTailBytesLeft);
            sendQueueTailBytesLeft = 0;
        }

        struct ibv_sge* SG = &(sendWRs.Segments[idx]);
        struct ibv_send_wr* SR = &(sendWRs.SendRequests[idx]);

        SG->addr = (uint64_t)sendData.get();
        SG->length = sendData.size();
        SR->sg_list = SG;

        if ((idx+1) % SendWRData::signalThreshold == 0) {
            SR->send_flags = IBV_SEND_SIGNALED;
        } else {
            SR->send_flags = 0;
        }

        if (i == toProcess - 1) {
            SR->next = nullptr;
        } else {
            SR->next = &(sendWRs.SendRequests[(baseIdx+i+1)%SendWRData::maxWR]);
        }

        outstandingBuffers[idx] = std::move(sendData);
    }

    stack->totalSend += toProcess;
    stack->sendBatchSum += toProcess;
    stack->sendBatchCount++;

    struct ibv_send_wr* badSR;
    if (int err = ibv_post_send(QP, firstSR, &badSR)) {
        K2ASSERT(false, "Failed to post send on RC QP: " << strerror(err));
        shutdownConnection();
        return;
    }

    sendWRs.postedCount += toProcess;
    queue.erase(queue.cbegin(), queue.cbegin()+toProcess);
    return;
}

future<Buffer> RDMAConnection::recv() {
    if (errorState) {
        return make_ready_future<Buffer>(Buffer());
    }

    if (recvPromiseActive) {
        K2ASSERT(false, "recv() called with promise already active");
        return make_exception_future<Buffer>(std::runtime_error("recv() called with promise already active"));
    }

    if (recvQueue.size()) {
        auto recv_future = make_ready_future<Buffer>(std::move(recvQueue.front()));
        recvQueue.pop_front();
        return recv_future;
    }

    recvPromise = promise<Buffer>();
    recvPromiseActive = true;
    return recvPromise.get_future();
}

void RDMAConnection::incomingMessage(char* data, uint32_t size) {
    K2DEBUG("RDMAConn " << QP->qp_num << " got message of size: " << size);
    Buffer buf(data, size, make_free_deleter(data));
    if (recvPromiseActive) {
        recvPromiseActive = false;
        recvPromise.set_value(std::move(buf));
    } else {
        recvQueue.push_back(std::move(buf));
    }

    if (!size) {
        // This is a graceful close message
        shutdownConnection();
    }
}

void RDMAConnection::send(std::vector<Buffer>&& buf) {
    if (!isReady || sendQueue.size()) {
        size_t beforeSize = sendQueue.size();

        auto it = buf.begin();
        for (; it != buf.end(); ++it) {
            if (it->size() > messagePackThreshold) {
                if (sendQueueTailBytesLeft > 0) {
                    sendQueue.back().trim(RDMAStack::RCDataSize-sendQueueTailBytesLeft);
                    sendQueueTailBytesLeft = 0;
                }
                break;
            }

            if (sendQueueTailBytesLeft < it->size()) {
                if (sendQueueTailBytesLeft > 0) {
                    sendQueue.back().trim(RDMAStack::RCDataSize-sendQueueTailBytesLeft);
                }
                sendQueue.emplace_back(RDMAStack::RCDataSize);
                sendQueueTailBytesLeft = RDMAStack::RCDataSize;
            }

            memcpy(sendQueue.back().get_write()+(RDMAStack::RCDataSize-sendQueueTailBytesLeft),
                    it->get(), it->size());
            sendQueueTailBytesLeft -= it->size();
        }

        if (it != buf.end()) {
            sendQueue.insert(sendQueue.end(), std::make_move_iterator(it),
                            std::make_move_iterator(buf.end()));
            sendQueueTailBytesLeft = 0;
        }

        stack->sendQueueSize += sendQueue.size() - beforeSize;
        return;
    }

    processSends<std::vector<Buffer>>(buf);
    if (buf.size()) {
        stack->sendQueueSize += buf.size();
        sendQueue.insert(sendQueue.end(), std::make_move_iterator(buf.begin()),
                         std::make_move_iterator(buf.end()));
    }
}

void RDMAConnection::shutdownConnection() {
    if (errorState) {
        return;
    }

    errorState = true;

    if (recvPromiseActive) {
        recvPromiseActive = false;
        recvPromise.set_value(Buffer());
    }

    if (sendQueue.size()) {
        K2WARN("Shutting down RC QP with pending sends");
    }

    if (QP) {
        (void) engine()._thread_pool->submit<int>([QP=this->QP]() {
            // Transitioning the QP into the Error state will flush
            // any outstanding WRs, possibly with errors. Is needed to
            // maintain consistency in the SRQ
            struct ibv_qp_attr attr = {};
            attr.qp_state = IBV_QPS_ERR;

            ibv_modify_qp(QP, &attr, IBV_QP_STATE);
            ibv_destroy_qp(QP);

            return 0;
        });
        QP = nullptr;
    }

    if (isReady) {
        stack->RCConnectionCount--;
        isReady = false;
    }

    // Connection will be removed from RCLookup and/or handshakeLookup
    // when they are referenced there and the weak_ptr is null
}

RDMAConnection::~RDMAConnection() noexcept {
    shutdownConnection();
}

RDMAListener RDMAStack::listen() {
    return RDMAListener(this);
}

RDMAStack::~RDMAStack() {
    for (auto it = RCLookup.begin(); it != RCLookup.end(); ++it) {
        if (it->second) {
            it->second->shutdownConnection();
        }
    }

    if (memRegionHandle) {
        ibv_dereg_mr(memRegionHandle);
        memRegionHandle = nullptr;
    }

    if (protectionDomain) {
        ibv_dealloc_pd(protectionDomain);
        protectionDomain = nullptr;
    }

    if (acceptPromiseActive) {
        acceptPromise.set_exception(std::runtime_error("RDMAStack destroyed with accept promise active"));
    }

    for (int i=0; i<RecvWRData::maxWR; ++i) {
        struct ibv_sge& SG = UDQPRRs.Segments[i];
        free((void*)SG.addr);
        struct ibv_sge& SRQSG = RCQPRRs.Segments[i];
        free((void*)SRQSG.addr);
    }
}

void RDMAStack::processCompletedSRs(std::array<Buffer, SendWRData::maxWR>& buffers, SendWRData& WRData, uint64_t signaledID) {
    uint32_t freed=0;
    K2ASSERT(WRData.postedIdx <= signaledID, "Send assumptions bad");
    for (int i=WRData.postedIdx; i<=(int)signaledID; ++i, ++freed) {
        buffers[i] = Buffer();
    }

    WRData.postedIdx = (WRData.postedIdx + freed) % SendWRData::maxWR;
    K2ASSERT(freed <= WRData.postedCount, "Bug in circular buffer for SRs");
    WRData.postedCount -= freed;
}

future<std::unique_ptr<RDMAConnection>> RDMAStack::accept() {
    if (acceptPromiseActive) {
        return make_exception_future<std::unique_ptr<RDMAConnection>>(std::runtime_error("accept() called while accept future still active"));
    }

    if (acceptQueue.size()) {
        auto accept_future = make_ready_future<std::unique_ptr<RDMAConnection>>(std::move(acceptQueue.front()));
        acceptQueue.pop_front();
        return accept_future;
    }

    acceptPromiseActive = true;
    acceptPromise = promise<std::unique_ptr<RDMAConnection>>();
    return acceptPromise.get_future();
}

std::unique_ptr<RDMAConnection> RDMAStack::connect(const EndPoint& remote) {
    std::unique_ptr<RDMAConnection> conn = std::make_unique<RDMAConnection>(this, remote);
    conn->makeHandshakeRequest();
    return conn;
}

bool RDMAStack::processRCCQ() {
    struct ibv_wc WCs[pollBatchSize];
    int completed = ibv_poll_cq(RCCQ, pollBatchSize, WCs);
    K2ASSERT(completed >= 0, "Failed to poll RC CQ");

    if (completed == 0) {
        return false;
    }

    int recvWCs = 0;
    struct ibv_recv_wr* prevRR = nullptr;
    struct ibv_recv_wr* firstRR = nullptr;

    for (int i=0; i<completed; ++i) {
        bool foundConn = true;
        auto connIt = RCLookup.find(WCs[i].qp_num);
        if (connIt == RCLookup.end()) {
            K2WARN("RCQP not found");
            foundConn = false;
        }
        if (foundConn && !connIt->second) {
            // This is the normal case for how connections are removed
            // from RCLookup
            K2DEBUG("RDMAConnection for RCQP was deleted");
            RCLookup.erase(connIt);
            foundConn = false;
        }

        // WC op code is not valid if there was an error, so we use the
        // wr_id to differentiate between sends and receives
        bool isRecv = WCs[i].wr_id < RecvWRData::maxWR;

        if (!isRecv) {
            // We can ignore send completions for connections that no longer exist
            if (!foundConn || !connIt->second->QP) {
                continue;
            }

            processCompletedSRs(connIt->second->outstandingBuffers,
                                connIt->second->sendWRs, WCs[i].wr_id - RecvWRData::maxWR);
            if (WCs[i].status != IBV_WC_SUCCESS) {
                K2WARN("error on send wc: " << ibv_wc_status_str(WCs[i].status));
                connIt->second->shutdownConnection();
            }

            size_t beforeSize = connIt->second->sendQueue.size();
            connIt->second->processSends(connIt->second->sendQueue);
            size_t afterSize = connIt->second->sendQueue.size();
            sendQueueSize -= beforeSize - afterSize;
        } else {
            int idx = WCs[i].wr_id;
            struct ibv_recv_wr& RR = RCQPRRs.RecvRequests[idx];
            ++recvWCs;

            if (WCs[i].status != IBV_WC_SUCCESS) {
                K2WARN("error on recv wc: " << ibv_wc_status_str(WCs[i].status));
                if (foundConn) {
                    connIt->second->shutdownConnection();
                }
                free((char*)RCQPRRs.Segments[idx].addr);
            } else if (foundConn) {
                char* data = (char*)RCQPRRs.Segments[idx].addr;
                uint32_t size = WCs[i].byte_len;
                connIt->second->incomingMessage(data, size);
            }

            // Prepare RR to be posted again. We cannot assume RRs are completed in order
            if (!firstRR) {
                firstRR = &RR;
            }
            if (prevRR) {
                prevRR->next = &RR;
            }
            prevRR = &RR;
            RCQPRRs.Segments[idx].addr = (uint64_t)malloc(RCDataSize);
            K2ASSERT(RCQPRRs.Segments[idx].addr, "Failed to allocate memory for RR");
        }
    }

    if (recvWCs) {
        totalRecv += recvWCs;
        recvBatchSum += recvWCs;
        ++recvBatchCount;

        prevRR->next = nullptr;
        struct ibv_recv_wr* badRR;
        if (int err = ibv_post_srq_recv(SRQ, firstRR, &badRR)) {
            K2ASSERT(false, "error on RC post_recv: " << strerror(err));
        }
    }

    return true;

}

bool RDMAStack::pollConnectionQueue() {
    struct rdma_cm_event *cm_event;

    while(true) {
        int ret = rdma_get_cm_event(connection_queue, &cm_event);
        if (ret && errno == EAGAIN) {
            return false;
        } else if (ret) {
            K2ASSERT(false, "error on connection queue poll: " << strerror(errno));
        }

        if (cm_event->status) {
            K2WARN("RDMA connection event error: " << strerror(cm_event->status));
            rdma_ack_cm_event(cm_event);
            continue;
        }

        if (cm_event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
        // TODO create QP, then accept
        }
        else if (cm_event->event == RDMA_CM_EVENT_ESTABLISHED) {
        }
        else if (cm_event->event == RDMA_CM_EVENT_REJECTED || cm_event->event == RDMA_CM_EVENT_CONNECT_ERROR) {
        }
        else if (cm_event->event == RDMA_CM_EVENT_DISCONNECTED) {
        }

        // Other events we ack but otherwise ignore
        rdma_ack_cm_event(cm_event);
    }
}

bool RDMAStack::poller() {
    sendQueueSum += sendQueueSize;
    ++sendQueueCount;

    bool didWork = processRCCQ();

    if (!didWork) {
        didWork = pollConnectionQueue();
    }

    return didWork;
}

void RDMAStack::registerPoller() {
    RDMAPoller = reactor::poller::simple([&] { return poller(); });
}

void RDMAStack::registerMetrics() {
    namespace sm = seastar::metrics;

    metricGroup.add_group("rdma_stack", {
        sm::make_counter("send_count", totalSend,
            sm::description("Total number of messages sent")),
        sm::make_counter("recv_count", totalRecv,
            sm::description("Total number of messages received")),
        sm::make_gauge("send_queue_size", [this]{
                if (!sendQueueCount) {
                    return 0.0;
                }
                double avg = sendQueueSum / (double)sendQueueCount;
                sendQueueSum = sendQueueCount = 0;
                return avg;
            },
            sm::description("Average size of the send queue")),
        sm::make_gauge("send_batch_size", [this]{
                if (!sendBatchCount) {
                    return 0.0;
                }
                double avg = sendBatchSum / (double)sendBatchCount;
                sendBatchSum = sendBatchCount = 0;
                return avg;
            },
            sm::description("Average size of the send batches")),
        sm::make_gauge("recv_batch_size", [this]{
                if (!recvBatchCount) {
                    return 0.0;
                }
                double avg = recvBatchSum / (double)recvBatchCount;
                recvBatchSum = recvBatchCount = 0;
                return avg;
            },
            sm::description("Average size of receive batches"))
    });
}

std::unique_ptr<RDMAStack> RDMAStack::makeRDMAStack(void* memRegion, size_t memRegionSize, std::string ip, uint16_t port) {
    std::unique_ptr<RDMAStack> stack = std::make_unique<RDMAStack>();

    struct in_addr local_addr;
    if (inet_aton(ip.c_str(), &local_addr) == 0) {
        K2ERROR("Failed to convert RDMA IP string: " << ip);
        return nullptr;
    }
    struct sockaddr_in local_sock;
    memset(&local_sock, 0, sizeof(struct sockaddr));
    local_sock.sin_port = htons(port);
    local_sock.sin_addr = local_addr;
    local_sock.sin_family = AF_INET;


    stack->connection_queue = rdma_create_event_channel();
    if (!cm_event_channel) {
        K2ERROR("Failed to create event channel: " << strerror(errno));
        return nullptr;
    }

    /* Set fd of connection queue to non-blocking so we can poll it instead of block */
    int fd_flags = fcntl(stack->connection_queue->fd, F_GETFL, 0);
    if (fd_flags < 0) {
        K2ERROR("Failed to get fd flags of event channel: " << strerror(errno));
        return nullptr;
    }
    fd_flags |= O_NONBLOCK;
    int fd_err = fcntl(stack->connection_queue->fd, F_SETFL, fd_flags);
    if (fd_err) {
        K2ERROR("Failed to set fd flags of event channel: " << strerror(errno));
        return nullptr;
    }


    /* rdma_cm_id is the connection identifier (like socket) which is used
     * to define an RDMA connection. This will be our listening socket
     */
    int ret = rdma_create_id(stack->connection_queue, &(stack->listen_id), NULL, RDMA_PS_TCP);
    if (ret) {
        K2ERROR("Failed to create rdma listen id: " << strerror(errno));
        return nullptr;
    }

    /* Explicit binding of rdma cm id to the socket credentials */
    ret = rdma_bind_addr(stack->listen_id, (struct sockaddr*) &local_sock);
    if (ret) {
        K2ERROR("Failed to bind address to rdma listen id: " << strerror(errno));
        return nullptr;
    }

    ret = rdma_listen(stack->listen_id, 64); /* backlog max = 64*/
    if (ret) {
        K2ERROR("Failed to start rdma listen: " << strerror(errno));
        return nullptr;
    }

    stack->protectionDomain = ibv_alloc_pd(stack->listen_id->verbs);
    if (!stack->protectionDomain) {
        K2ERROR("ibv_alloc_pd failed");
        return nullptr;
    }

    stack->memRegionHandle = ibv_reg_mr(stack->protectionDomain, memRegion, memRegionSize, IBV_ACCESS_LOCAL_WRITE);
    if (!stack->memRegionHandle) {
        K2ERROR("Failed to register memory: " << strerror(errno));
        return nullptr;
    }

    stack->localEndpoint = Endpoint(local_sock);

    stack->RCCQ = ibv_create_cq(stack->listen_id->verbs, RCCQSize, nullptr, nullptr, 0);
    if (!stack->RCCQ) {
        K2ERROR("Failed to create RCCQ: " << strerror(errno));
        return nullptr;
    }

    struct ibv_srq_init_attr SRQAttr = {
        .srq_context = nullptr,
        .attr = {
            .max_wr = RecvWRData::maxWR,
            .max_sge = 1,
            .srq_limit = 0}
    };
    stack->SRQ = ibv_create_srq(stack->protectionDomain, &SRQAttr);
    if (!stack->SRQ) {
        K2ERROR("Failed to create SRQ: " << strerror(errno));
        return nullptr;
    }

    for (int i=0; i<RecvWRData::maxWR; ++i) {
        struct ibv_recv_wr& RR = stack->RCQPRRs.RecvRequests[i];
        struct ibv_sge& SG = stack->RCQPRRs.Segments[i];

        RR.wr_id = i;
        if (i == RecvWRData::maxWR-1) {
            RR.next = nullptr;
        } else {
            RR.next = &(stack->RCQPRRs.RecvRequests[i+1]);
        }
        // When an incoming message is received, this malloc'ed memory
        // will be wrapped in a temporary_buffer and passed to the user
        // so it will be freed when the user drops the temporary_buffer
        SG.addr = (uint64_t)malloc(RCDataSize);
        K2ASSERT(SG.addr, "Failed to allocate memory for RR");
        SG.length = RCDataSize;
        SG.lkey = stack->memRegionHandle->lkey;
        RR.sg_list = &SG;
        RR.num_sge = 1;
    }
    struct ibv_recv_wr* badRR;
    if (int err = ibv_post_srq_recv(stack->SRQ, stack->RCQPRRs.RecvRequests, &badRR)) {
        K2ERROR("failed to post SRQ RRs: " << strerror(err));
        return nullptr;
    }

    stack->registerPoller();
    stack->registerMetrics();

    return stack;
}

} // namespace rdma
} // namespace seastar
