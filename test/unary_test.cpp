#include "common.h"
#include "unary_call.h"
#include <future>
#include <gtest/gtest.h>
#include <memory>

TEST_F(UnaryFixture, IdealScenario) {
    StartServer();
    ConnectClientStubToServer();
    auto client_thread = std::async([&] {
        CompletionQueuePuller puller(client_->CompletionQueue());
        StringMsg request;
        request.set_text("REQUEST");
        auto call = StartClientCall(request);
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::OutgoingCall, true, grpc::CompletionQueue::GOT_EVENT);
        EXPECT_EQ(call->response_.text(), "RESPONSE");
        EXPECT_TRUE(call->finish_status_.ok());
    });
    auto server_thread = std::async([&] {
        CompletionQueuePuller puller(server_->CompletionQueue());
        auto call = StartServerRequest();
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::IncomingCall, true, grpc::CompletionQueue::GOT_EVENT);
        EXPECT_EQ(call->request_.text(), "REQUEST");
        StringMsg response;
        response.set_text("RESPONSE");
        call->Finish(response);
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
    });
    client_thread.wait();
    server_thread.wait();
}

TEST_F(UnaryFixture, ClientCancelsCall) {
    StartServer();
    ConnectClientStubToServer();
    CompletionQueuePuller client_puller(client_->CompletionQueue());
    StringMsg request;
    request.set_text("REQUEST");
    auto client_call = StartClientCall(request);

    CompletionQueuePuller server_puller(server_->CompletionQueue());
    auto server_call = StartServerRequest();
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);

    client_call->TryCancel();
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);
    EXPECT_EQ(client_call->finish_status_.error_code(), grpc::StatusCode::CANCELLED);

    StringMsg response;
    response.set_text("RESPONSE");
    server_call->Finish(response);
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
}

TEST_F(UnaryFixture, StopServerWithPendingRequest) {
    StartServer();
    auto server_call = StartServerRequest();
    server_.reset();
    server_call.reset();
}
