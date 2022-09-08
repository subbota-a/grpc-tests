#include "common.h"
#include <chrono>
#include <future>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include <magic_enum.hpp>
#include <memory>

using mypkg::CountMsg;
using mypkg::MyService;
using mypkg::StringMsg;

namespace {

class ServerCall final {
    grpc::ServerContext context_;
    std::unique_ptr<grpc::ServerAsyncWriter<StringMsg>> writer_;
public:
    CountMsg request_;
    explicit ServerCall(Server &server, bool async_done) : writer_(std::make_unique<grpc::ServerAsyncWriter<StringMsg>>(&context_)) {
        if (async_done) {
            context_.AsyncNotifyWhenDone(reinterpret_cast<void *>(Operation::AsyncDone));
        }
        server.RequestServerStream(&context_, &request_, writer_.get(),
                                   reinterpret_cast<void *>(Operation::IncomingCall));
    }
    ~ServerCall(){
        context_.TryCancel();
    }
    void Write(const StringMsg &sentMessage) {
        writer_->Write(sentMessage, reinterpret_cast<void *>(Operation::WriteCall));
    }
    void Finish(const grpc::Status& status) { writer_->Finish(status, reinterpret_cast<void *>(Operation::FinishCall)); }
    void SendMessagesUntilOk(CompletionQueuePuller &puller, int max_count, int &sentMessageCount,
                             std::optional<std::chrono::milliseconds> delay = {}) {
        ::SendMessagesUntilOk(*writer_, puller, max_count, sentMessageCount, delay);
    }

};

class ClientCall final {
    grpc::ClientContext context_;
    std::unique_ptr<grpc::ClientAsyncReader<StringMsg>> reader_;
public:
    grpc::Status finish_status_;
    explicit ClientCall(Client &client, const CountMsg &request,
                        std::optional<std::chrono::system_clock::time_point> deadline) {
        if (deadline)
            context_.set_deadline(*deadline);
        reader_ = client.AsyncServerStream(&context_, request, reinterpret_cast<void *>(Operation::OutgoingCall));
    }
    ~ClientCall() {
        context_.TryCancel();
    }
    void Read(StringMsg *readMessage) {
        reader_->Read(readMessage, reinterpret_cast<void *>(Operation::ReadCall));
    }
    void ReadMessagesUntilOk(StringMsg *readMessage, CompletionQueuePuller &puller, int &readMessageCount,
                             int maxReadCount) {
        ::ReadMessagesUntilOk(*reader_, readMessage, puller, readMessageCount, maxReadCount);
    }
    void Finish() {
        reader_->Finish(&finish_status_, reinterpret_cast<void *>(Operation::FinishCall));
    }
};
}// namespace

class ServerStreamFixture : public BaseFixture {
protected:
    [[nodiscard]] std::unique_ptr<ServerCall> StartServerStream(bool async_done=false) { return std::make_unique<ServerCall>(*server_, async_done); }
    [[nodiscard]] std::unique_ptr<ClientCall>
    StartClientCall(const CountMsg &request, std::optional<std::chrono::system_clock::time_point> deadline = {}) {
        return std::make_unique<ClientCall>(*client_, request, deadline);
    }
};

TEST_F(ServerStreamFixture, IdealScenario) {
    StartServer();
    ConnectClientStubToServer();

    const int requested_count = 1000;
    auto client_thread = std::async([&]{
        CompletionQueuePuller client_puller(client_->CompletionQueue());
        CountMsg request;
        request.set_num(requested_count);
        auto call = StartClientCall(request);
        ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true, grpc::CompletionQueue::GOT_EVENT);
        StringMsg read_msg;
        int read_count = 0;
        call->ReadMessagesUntilOk(&read_msg, client_puller, read_count, INT_MAX);
        EXPECT_EQ(read_count, requested_count);
        call->Finish();
        ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
        EXPECT_TRUE(call->finish_status_.ok());
    });
    auto server_thread = std::async([&]{
        CompletionQueuePuller server_puller(server_->CompletionQueue());
        auto call = StartServerStream();
        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true, grpc::CompletionQueue::GOT_EVENT);
        EXPECT_EQ(call->request_.num(), requested_count);
        int sent_count = 0;
        call->SendMessagesUntilOk(server_puller, call->request_.num(), sent_count);
        EXPECT_EQ(call->request_.num(), sent_count);
        EXPECT_TRUE(server_puller.ok());
        call->Finish(grpc::Status());
        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
    });
    client_thread.wait();
    server_thread.wait();
}

TEST_F(ServerStreamFixture, ClientCancelStream) {
    StartServer();
    ConnectClientStubToServer();

    const int requested_count = 1000;

    CompletionQueuePuller client_puller(client_->CompletionQueue());
    CountMsg request;
    request.set_num(requested_count);
    auto client_call = StartClientCall(request);
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true, grpc::CompletionQueue::GOT_EVENT);

    CompletionQueuePuller server_puller(server_->CompletionQueue());
    auto server_call = StartServerStream();
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true, grpc::CompletionQueue::GOT_EVENT);

    StringMsg write_msg;
    StringMsg read_msg;
    server_call->Write(write_msg);
    client_call->Read(&read_msg);
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::ReadCall, true, grpc::CompletionQueue::GOT_EVENT);
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::WriteCall, true, grpc::CompletionQueue::GOT_EVENT);

    client_call.reset();
    int sent_count = 0;
    server_call->SendMessagesUntilOk(server_puller, INT_MAX, sent_count);
    ASSERT_EQ(server_puller.status(), grpc::CompletionQueue::NextStatus::GOT_EVENT);
    ASSERT_LT(sent_count, INT_MAX);
    server_call->Finish(grpc::Status());
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::FinishCall, false, grpc::CompletionQueue::GOT_EVENT);
}
