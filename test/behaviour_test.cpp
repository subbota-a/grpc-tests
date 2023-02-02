#ifdef _WINDOWS
#include <windows.h>
#undef max
#endif

#include "common.h"
#include "unary_call.h"
#include <chrono>
#include <future>

using namespace std::chrono_literals;
const size_t PendingIncomingRequestCount = 100'000;

class BehaviourFixture : public UnaryFixture {
protected:
    void SetUp() override {
        UnaryFixture::SetUp();
#ifdef _WINDOWS
        //ASSERT_TRUE(SetProcessAffinityMask(GetCurrentProcess(), 0x3));
#endif
    }
};

TEST_F(BehaviourFixture, ServerPutsManyNewRequestsToQueue) {
    StartServer();
    ConnectClientStubToServer();

    CompletionQueuePuller client_puller(client_->CompletionQueue(), 1s);
    CompletionQueuePuller server_puller(server_->CompletionQueue(), 1s);

    const unsigned parallel_requests = 1;
    for (unsigned i = 0; i < parallel_requests; ++i) {
        StartServerRequest().release();// NOLINT(bugprone-unused-return-value)
    }

    StringMsg request;
    request.set_text("REQUEST");
    for (size_t i = 0; i < PendingIncomingRequestCount; ++i) {
        StartClientCall(request).release();// NOLINT(bugprone-unused-return-value)
    }

    StringMsg response;
    response.set_text("RESPONSE");
    unsigned finished_clients_requests = 0;
    unsigned active_server_requests = parallel_requests;
    while (finished_clients_requests < PendingIncomingRequestCount) {
        ASSERT_EQ(server_puller.Pull(), grpc::CompletionQueue::NextStatus::GOT_EVENT);
        ASSERT_TRUE(server_puller.ok());
        auto server_call = static_cast<UnaryServerCall *>(server_puller.call());
        switch (server_puller.tag()) {
        case Operation::IncomingCall: server_call->Finish(response); break;
        case Operation::FinishCall:
            delete server_call;
            ++finished_clients_requests;
            --active_server_requests;
            if (finished_clients_requests + active_server_requests < PendingIncomingRequestCount) {
                StartServerRequest().release();// NOLINT(bugprone-unused-return-value)
                ++active_server_requests;
            }
            break;
        default: FAIL();
        }
    }

    for (size_t i = 0; i < PendingIncomingRequestCount; ++i) {
        SCOPED_TRACE("client " + std::to_string(i));
        ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        auto client_call = static_cast<UnaryClientCall *>(client_puller.call());
        delete client_call;
    }
}

TEST_F(BehaviourFixture, ServerServeRequestImmideatly) {
    StartServer();
    ConnectClientStubToServer();

    CompletionQueuePuller client_puller(client_->CompletionQueue(), 1s);
    CompletionQueuePuller server_puller(server_->CompletionQueue(), 1s);

    StringMsg request;
    request.set_text("REQUEST");
    StringMsg response;
    response.set_text("RESPONSE");
    for (size_t i = 0; i < PendingIncomingRequestCount; ++i) {
        auto incoming_call = StartServerRequest();
        auto client_call = StartClientCall(request);
        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        incoming_call->Finish(response);
        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::FinishCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
    }
}

TEST_F(BehaviourFixture, OptimizedScenario) {
    StartServer();
    ConnectClientStubToServer();

    CompletionQueuePuller client_puller(client_->CompletionQueue(), 1s);
    CompletionQueuePuller server_puller(server_->CompletionQueue(), 1s);

#pragma clang diagnostic push
#pragma ide diagnostic ignored "UnusedValue"
    [[maybe_unused]] auto server_thread = std::async(std::launch::async, [&] {
        StringMsg response;
        response.set_text("RESPONSE");
        StartServerRequest().release();// NOLINT(bugprone-unused-return-value)
        auto incoming_calls_count = 1;
        for (size_t i = 0; i < PendingIncomingRequestCount;) {
            ASSERT_EQ(server_puller.Pull(), grpc::CompletionQueue::GOT_EVENT);
            ASSERT_TRUE(server_puller.ok());
            auto incoming_call = static_cast<UnaryServerCall *>(server_puller.call());
            switch (server_puller.tag()) {
            case Operation::IncomingCall:
                ASSERT_TRUE(server_puller.ok());
                incoming_call->Finish(response);
                if (incoming_calls_count < PendingIncomingRequestCount) {
                    StartServerRequest().release();// NOLINT(bugprone-unused-return-value)
                    ++incoming_calls_count;
                }
                break;
            case Operation::FinishCall:
                delete incoming_call;
                ++i;
                break;
            default:
                break;
            }
        }
    });

    [[maybe_unused]] auto client_reader = std::async(std::launch::async, [&] {
        for (size_t i = 0; i < PendingIncomingRequestCount; ++i) {
            SCOPED_TRACE("client " + std::to_string(i));
            ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                                grpc::CompletionQueue::GOT_EVENT);
            auto client_call = static_cast<UnaryClientCall *>(client_puller.call());
            delete client_call;
        }
    });
#pragma clang diagnostic pop

    StringMsg request;
    request.set_text("REQUEST");
    for (size_t i = 0; i < PendingIncomingRequestCount; ++i) {
        StartClientCall(request).release();// NOLINT(bugprone-unused-return-value)
    }
}
