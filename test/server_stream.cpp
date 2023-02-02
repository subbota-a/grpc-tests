#include "common.h"
#include <chrono>
#include <future>
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
    CompletionQueueTag tag_;
    CompletionQueueTag done_tag_;

public:
    CountMsg request_;
    explicit ServerCall(Server &server, bool async_done)
        : writer_(std::make_unique<grpc::ServerAsyncWriter<StringMsg>>(&context_)), tag_{this, IncomingCall},
          done_tag_{this, Operation::AsyncDone} {
        if (async_done) {
            context_.AsyncNotifyWhenDone(&done_tag_);
        }
        server.RequestServerStream(&context_, &request_, writer_.get(), &tag_);
    }
    ~ServerCall() { context_.TryCancel(); }
    void Write(const StringMsg &sentMessage) {
        tag_.operation = Operation::WriteCall;
        writer_->Write(sentMessage, &tag_);
    }
    void Finish(const grpc::Status &status) {
        tag_.operation = Operation::FinishCall;
        writer_->Finish(status, &tag_);
    }
    void SendMessagesUntilOk(CompletionQueuePuller &puller, int max_count, int &sentMessageCount,
                             std::optional<std::chrono::milliseconds> delay = {}) {
        ::SendMessagesUntilOk(*writer_, puller, &tag_, max_count, sentMessageCount, delay);
    }
};

class ClientCall final {
    grpc::ClientContext context_;
    std::unique_ptr<grpc::ClientAsyncReader<StringMsg>> reader_;
    CompletionQueueTag tag_;

public:
    grpc::Status finish_status_;
    explicit ClientCall(Client &client, const CountMsg &request,
                        std::optional<std::chrono::system_clock::time_point> deadline)
        : tag_{this, Operation::OutgoingCall} {
        if (deadline)
            context_.set_deadline(*deadline);
        reader_ = client.AsyncServerStream(&context_, request, &tag_);
    }
    void TryCancel() { context_.TryCancel(); }
    void Read(StringMsg *readMessage) {
        tag_.operation = Operation::ReadCall;
        reader_->Read(readMessage, &tag_);
    }
    void ReadMessagesUntilOk(StringMsg *readMessage, CompletionQueuePuller &puller, int &readMessageCount,
                             int maxReadCount) {
        ::ReadMessagesUntilOk(*reader_, readMessage, puller, &tag_, readMessageCount, maxReadCount);
    }
    void Finish() {
        tag_.operation = Operation::FinishCall;
        reader_->Finish(&finish_status_, &tag_);
    }
};

enum class TimeoutScenario {
    NotExpired,
    ExpiredBeforeRequest,
    ExpiredBeforeAccept,
    ExpiredBeforeWrite,
    ExpiredBeforeRead,
    ExpiredBeforeServerFinish,
    ExpiredBeforeClientReceivesFinish
};

std::ostream &operator<<(std::ostream &os, TimeoutScenario value) { return os << magic_enum::enum_name(value); }

}// namespace

class ServerStreamFixture : public BaseFixture {
protected:
    [[nodiscard]] std::unique_ptr<ServerCall> StartServerStream(bool async_done = false) {
        return std::make_unique<ServerCall>(*server_, async_done);
    }
    [[nodiscard]] std::unique_ptr<ClientCall>
    StartClientCall(const CountMsg &request, std::optional<std::chrono::system_clock::time_point> deadline = {}) {
        return std::make_unique<ClientCall>(*client_, request, deadline);
    }
};

TEST_F(ServerStreamFixture, IdealScenario) {
    StartServer();
    ConnectClientStubToServer();

    const int requested_count = 100'000;
    auto client_thread = std::async([&] {
        CompletionQueuePuller client_puller(client_->CompletionQueue());
        CountMsg request;
        request.set_num(requested_count);
        auto call = StartClientCall(request);
        ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        StringMsg read_msg;
        int read_count = 0;
        call->ReadMessagesUntilOk(&read_msg, client_puller, read_count, INT_MAX);
        EXPECT_EQ(read_count, requested_count);
        call->Finish();
        ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        EXPECT_TRUE(call->finish_status_.ok());
    });
    auto server_thread = std::async([&] {
        CompletionQueuePuller server_puller(server_->CompletionQueue());
        auto call = StartServerStream();
        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        EXPECT_EQ(call->request_.num(), requested_count);
        int sent_count = 0;
        call->SendMessagesUntilOk(server_puller, call->request_.num(), sent_count);
        EXPECT_EQ(call->request_.num(), sent_count);
        EXPECT_TRUE(server_puller.ok());
        call->Finish(grpc::Status());
        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::FinishCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
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
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);

    CompletionQueuePuller server_puller(server_->CompletionQueue());
    auto server_call = StartServerStream();
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);

    StringMsg write_msg;
    StringMsg read_msg;
    server_call->Write(write_msg);
    client_call->Read(&read_msg);
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::ReadCall, true, grpc::CompletionQueue::GOT_EVENT);
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::WriteCall, true, grpc::CompletionQueue::GOT_EVENT);

    client_call->Finish();
    client_call->TryCancel();
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
    client_call.reset();
    int sent_count = 0;
    server_call->SendMessagesUntilOk(server_puller, INT_MAX, sent_count);
    ASSERT_EQ(server_puller.status(), grpc::CompletionQueue::NextStatus::GOT_EVENT);
    ASSERT_LT(sent_count, INT_MAX);
    server_call->Finish(grpc::Status());
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::FinishCall, false,
                        grpc::CompletionQueue::GOT_EVENT);
}

class ServerStreamFixtureTimeout : public ServerStreamFixture, public ::testing::WithParamInterface<TimeoutScenario> {};

INSTANTIATE_TEST_SUITE_P(ScenarioArguments, ServerStreamFixtureTimeout,
                         testing::Values(TimeoutScenario::ExpiredBeforeRequest, TimeoutScenario::ExpiredBeforeAccept,
                                         TimeoutScenario::ExpiredBeforeWrite, TimeoutScenario::ExpiredBeforeRead,
                                         TimeoutScenario::ExpiredBeforeServerFinish,
                                         TimeoutScenario::ExpiredBeforeClientReceivesFinish,
                                         TimeoutScenario::NotExpired));

TEST_P(ServerStreamFixtureTimeout, Timeout) {
    using namespace std::chrono_literals;
    StartServer();
    ConnectClientStubToServer();
    auto start = std::chrono::system_clock::now();
    const auto context_timeout = 50ms;
    bool expired = false;

    CompletionQueuePuller client_puller(client_->CompletionQueue());

    if (GetParam() == TimeoutScenario::ExpiredBeforeRequest) {
        std::this_thread::sleep_until(start + 2 * context_timeout);
        expired = true;
    }

    CountMsg request;
    auto client_call = StartClientCall(request, start + context_timeout);
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, !expired,
                        grpc::CompletionQueue::GOT_EVENT);
    if (client_puller.ok()) {
        CompletionQueuePuller server_puller(server_->CompletionQueue());
        if (GetParam() == TimeoutScenario::ExpiredBeforeAccept) {
            if (grpc::Version() == "1.27.2") {
                return;
            }
            std::this_thread::sleep_until(start + 2 * context_timeout);
            expired = true;
        }
        auto server_call = StartServerStream();
        // server doesn't sensitive to deadline in case of accept request
        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        if (server_puller.ok()) {
            if (GetParam() == TimeoutScenario::ExpiredBeforeWrite) {
                std::this_thread::sleep_until(start + 2 * context_timeout);
                expired = true;
            }
            StringMsg write_msg;
            server_call->Write(write_msg);
            ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::WriteCall, !expired,
                                grpc::CompletionQueue::GOT_EVENT);
        }
        if (GetParam() == TimeoutScenario::ExpiredBeforeRead) {
            std::this_thread::sleep_until(start + 2 * context_timeout);
            expired = true;
        }

        StringMsg read_msg;
        client_call->Read(&read_msg);
        ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::ReadCall, !expired,
                            grpc::CompletionQueue::GOT_EVENT);

        if (GetParam() == TimeoutScenario::ExpiredBeforeServerFinish) {
            std::this_thread::sleep_until(start + 2 * context_timeout);
            expired = true;
        }
        server_call->Finish(grpc::Status());
        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::FinishCall, !expired,
                            grpc::CompletionQueue::GOT_EVENT);
        if (GetParam() == TimeoutScenario::ExpiredBeforeClientReceivesFinish) {
            std::this_thread::sleep_until(start + 2 * context_timeout);
            expired = true;
        }

        client_call->Finish();
        ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        if (expired) {
            EXPECT_EQ(client_call->finish_status_.error_code(), grpc::StatusCode::DEADLINE_EXCEEDED);
        } else {
            EXPECT_TRUE(client_call->finish_status_.ok());
        }
    }
}
