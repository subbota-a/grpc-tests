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

class BehaviourFixture : public UnaryFixture{
protected:
    void SetUp() override {
        UnaryFixture::SetUp();
#ifdef _WINDOWS
        ASSERT_TRUE(SetProcessAffinityMask(GetCurrentProcess(), 0x3));
#endif
    }
};

TEST_F(BehaviourFixture, ServerPutsNewRequestsToQueue) {
    StartServer();
    ConnectClientStubToServer();

    CompletionQueuePuller client_puller(client_->CompletionQueue(), 1s);
    CompletionQueuePuller server_puller(server_->CompletionQueue(), 1s);

    auto incoming_call = StartServerRequest();

    std::vector<std::unique_ptr<UnaryClientCall>> outgoing_calls;
    StringMsg request;
    request.set_text("REQUEST");
    for (size_t i = 0; i < PendingIncomingRequestCount; ++i) {
        outgoing_calls.push_back(StartClientCall(request));
    }

    StringMsg response;
    response.set_text("RESPONSE");
    for (size_t i = 0; i < PendingIncomingRequestCount; ++i) {
        SCOPED_TRACE("server " + std::to_string(i));
        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        incoming_call->Finish(response);
        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::FinishCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        if (i + 1 < PendingIncomingRequestCount) {
            incoming_call = StartServerRequest();
        }
    }

    for (size_t i = 0; i < PendingIncomingRequestCount; ++i) {
        SCOPED_TRACE("client " + std::to_string(i));
        ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                            grpc::CompletionQueue::GOT_EVENT);

    }
    outgoing_calls.clear();
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

    auto server_thread = std::async(std::launch::async, [&]{
        StringMsg response;
        response.set_text("RESPONSE");
        auto incoming_call = StartServerRequest();
        auto incoming_calls_count = 1;
        std::vector<std::unique_ptr<UnaryServerCall>> finishing_calls;
        for (size_t i = 0; i < PendingIncomingRequestCount; ) {
            ASSERT_EQ(server_puller.Pull(), grpc::CompletionQueue::GOT_EVENT);
            switch (server_puller.tag()){
            case Operation::IncomingCall:
                ASSERT_TRUE(server_puller.ok());
                incoming_call->Finish(response);
                finishing_calls.push_back(std::move(incoming_call));
                if (incoming_calls_count<PendingIncomingRequestCount) {
                    incoming_call = StartServerRequest();
                    ++incoming_calls_count;
                }
                break;
            case Operation::FinishCall:
                ASSERT_TRUE(server_puller.ok());
                ++i;
                break;
            }
        }
    });

    std::vector<std::unique_ptr<UnaryClientCall>> outgoing_calls;
    auto client_reader = std::async(std::launch::async, [&]{
        for (size_t i = 0; i < PendingIncomingRequestCount; ++i) {
            SCOPED_TRACE("client " + std::to_string(i));
            ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                                grpc::CompletionQueue::GOT_EVENT);

        }
        outgoing_calls.clear();
    });

    StringMsg request;
    request.set_text("REQUEST");
    for (size_t i = 0; i < PendingIncomingRequestCount; ++i) {
        outgoing_calls.push_back(StartClientCall(request));
    }


}
