#include "common.h"
#include <chrono>
#include <future>
#include <gtest/gtest.h>
#include <magic_enum.hpp>
#include <memory>

using mypkg::MyService;
using mypkg::StringMsg;

namespace {
class ClientCall final {
    grpc::ClientContext context_;
    std::unique_ptr<grpc::ClientAsyncResponseReader<mypkg::StringMsg>> reader_;

public:
    grpc::Status finish_status_;
    StringMsg response_;

    ClientCall(Client &client, const StringMsg &request,
               std::optional<std::chrono::system_clock::time_point> deadline) {
        if (deadline)
            context_.set_deadline(*deadline);
        reader_ = client.AsyncUnary(&context_, request);
        reader_->Finish(&response_, &finish_status_, reinterpret_cast<void *>(Operation::OutgoingCall));
    }
    void TryCancel() { context_.TryCancel(); }
};

class ServerCall final {
    grpc::ServerContext context_;
    std::unique_ptr<grpc::ServerAsyncResponseWriter<StringMsg>> writer_;

public:
    StringMsg request_;

    explicit ServerCall(Server &server, bool async_done)
        : writer_(std::make_unique<grpc::ServerAsyncResponseWriter<StringMsg>>(&context_)) {
        if (async_done) {
            context_.AsyncNotifyWhenDone(reinterpret_cast<void *>(Operation::AsyncDone));
        }
        server.RequestUnary(&context_, &request_, writer_.get(),
                                   reinterpret_cast<void *>(Operation::IncomingCall));
    }
    ~ServerCall() { context_.TryCancel(); }
    void Finish(const StringMsg& response) {
        writer_->Finish(response, grpc::Status(), reinterpret_cast<void *>(Operation::FinishCall));
    }
};

}// namespace
class UnaryFixture : public BaseFixture {
protected:
    [[nodiscard]] std::unique_ptr<ServerCall> StartServerRequest(bool async_done = false) {
        return std::make_unique<ServerCall>(*server_, async_done);
    }
    [[nodiscard]] std::unique_ptr<ClientCall>
    StartClientCall(const StringMsg &request, std::optional<std::chrono::system_clock::time_point> deadline = {}) {
        return std::make_unique<ClientCall>(*client_, request, deadline);
    }
};

TEST_F(UnaryFixture, IdealScenario) {
    StartServer();
    ConnectClientStubToServer();
    auto client_thread = std::async([&] {
        CompletionQueuePuller puller(client_->CompletionQueue());
        StringMsg request;
        request.set_text("REQUEST");
        auto call = StartClientCall(request);
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::OutgoingCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        EXPECT_EQ(call->response_.text(), "RESPONSE");
        EXPECT_TRUE(call->finish_status_.ok());
    });
    auto server_thread = std::async([&] {
        CompletionQueuePuller puller(server_->CompletionQueue());
        auto call = StartServerRequest();
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::IncomingCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        EXPECT_EQ(call->request_.text(), "REQUEST");
        StringMsg response;
        response.set_text("RESPONSE");
        call->Finish(response);
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::FinishCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
    });
    client_thread.wait();
    server_thread.wait();
}
