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
    grpc::ServerAsyncReader<CountMsg, StringMsg> serverReader_;

public:
    explicit ServerCall(Server &server) : serverReader_(&context_) {
        server.RequestClientStream(&context_, &serverReader_, reinterpret_cast<void *>(Operation::IncomingCall));
    }

    ~ServerCall() { context_.TryCancel(); }

    void Read(StringMsg *readMessage) {
        serverReader_.Read(readMessage, reinterpret_cast<void *>(Operation::ReadCall));
    }

    void ReadMessagesUntilOk(StringMsg *readMessage, CompletionQueuePuller &puller, int &readMessageCount,
                             int maxReadCount) {
        ::ReadMessagesUntilOk(serverReader_, readMessage, puller, readMessageCount, maxReadCount);
    }

    void FinishServerRpc(int finish_num) {
        grpc::Status okStatus;
        CountMsg serverResponse;
        serverResponse.set_num(finish_num);
        serverReader_.Finish(serverResponse, okStatus, reinterpret_cast<void *>(Operation::FinishCall));
    }
};

class ClientCall final {
    grpc::ClientContext context_;
    std::unique_ptr<grpc::ClientAsyncWriter<StringMsg>> clientWriter_;

public:
    CountMsg response_;
    grpc::Status responseStatus;

    ClientCall(Client &client, std::optional<std::chrono::system_clock::time_point> deadline) {
        if (deadline)
            context_.set_deadline(*deadline);
        clientWriter_ =
            client.AsyncClientStream(&context_, &response_, reinterpret_cast<void *>(Operation::OutgoingCall));
    }
    ~ClientCall() { context_.TryCancel(); }

    void Write(const StringMsg &sentMessage) {
        clientWriter_->Write(sentMessage, reinterpret_cast<void *>(Operation::WriteCall));
    }
    void WritesDone() { clientWriter_->WritesDone(reinterpret_cast<void *>(Operation::WriteDone)); }
    void SendMessagesUntilOk(CompletionQueuePuller &puller, int max_count, int &sentMessageCount,
                             std::optional<std::chrono::milliseconds> delay = {}) {
        ::SendMessagesUntilOk(*clientWriter_, puller, max_count, sentMessageCount, delay);
    }
    void Finish() { clientWriter_->Finish(&responseStatus, reinterpret_cast<void *>(Operation::FinishCall)); }
};

enum class TimeoutScenario {
    NotExpired,
    ExpiredBeforeRequest,
    ExpiredBeforeAccept,
    ExpiredBeforeWrite,
    ExpiredBeforeRead,
    ExpiredBeforeWritesDone,
    ExpiredBeforeReadDone,
    ExpiredBeforeServerFinish,
    ExpiredBeforeClientReceivesFinish
};

std::ostream &operator<<(std::ostream &os, TimeoutScenario value) { return os << magic_enum::enum_name(value); }

}// namespace

class ClientStreamFixture : public BaseFixture {
protected:
    void TearDown() override {
        serverCall_.reset();
        clientCall_.reset();
        BaseFixture::TearDown();
    }
    void StartServerWaiting() { serverCall_ = std::make_unique<ServerCall>(*server_); }
    void StartClientCall(std::optional<std::chrono::system_clock::time_point> deadline = {}) {
        clientCall_ = std::make_unique<ClientCall>(*client_, deadline);
    }

protected:
    std::unique_ptr<ServerCall> serverCall_;
    std::unique_ptr<ClientCall> clientCall_;
};

TEST_F(ClientStreamFixture, CheckNoGrpcByteStreamAssert) {
    // In version 1.45 there was introduced a bug
    // Debug version fails here: byte_stream.cc:62] assertion failed: backing_buffer_.count > 0
    // Release version fails here: slice_buffer.cc:384] assertion failed: sb->count > 0
    StartServer();
    ConnectClientStubToServer();
    CompletionQueuePuller server_puller(server_->CompletionQueue());
    CompletionQueuePuller client_puller(client_->CompletionQueue());

    // start stream from client to server and wait for completion
    StartClientCall();
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);

    // start waiting for client stream and wait for completion
    StartServerWaiting();
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);
    // start reading the message
    StringMsg readMessage;
    serverCall_->Read(&readMessage);
    // start writing the message
    StringMsg sentMessage;
    clientCall_->Write(sentMessage);
    // wait for write completed
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::WriteCall, true, grpc::CompletionQueue::GOT_EVENT);
    // wait for read completed
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::ReadCall, true, grpc::CompletionQueue::GOT_EVENT);

    // finish server stream
    const int finish_num = 2124146;// random number
    serverCall_->FinishServerRpc(finish_num);
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
    serverCall_.reset();

    // client doesn't know about it and goes on writing until ok is true
    // here gRPC fails on assert!
    int sentMessageCount = 0;
    clientCall_->SendMessagesUntilOk(client_puller, INT_MAX, sentMessageCount);
    EXPECT_LT(sentMessageCount, INT_MAX);

    clientCall_->Finish();
    EXPECT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
}

TEST_F(ClientStreamFixture, IdealScenario) {
    StartServer();
    ConnectClientStubToServer();
    CompletionQueuePuller server_puller(server_->CompletionQueue());
    CompletionQueuePuller client_puller(client_->CompletionQueue());

    const int no_response_num = -1;
    const int response_num = 2124146;// random number

    // Client gets write
    StartClientCall();
    // The call is successful even if server doesn't wait for the call.
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);

    // Server makes themselves ready for getting the call
    StartServerWaiting();
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);

    const int maxSentMessageCount = 1000;
    auto client_done = std::async([&] {
        // Client can request for finish at any moment, it doesn't close the client stream.
        clientCall_->Finish();

        int sentMessageCount = 0;
        clientCall_->SendMessagesUntilOk(client_puller, maxSentMessageCount, sentMessageCount);
        EXPECT_EQ(maxSentMessageCount, sentMessageCount);

        clientCall_->WritesDone();
        // there are two pending requests in the queue
        EXPECT_EQ(client_puller.Pull(), grpc::CompletionQueue::GOT_EVENT);
        EXPECT_TRUE(client_puller.ok());
        EXPECT_THAT(client_puller.tag(), ::testing::AnyOf(Operation::WriteDone, Operation::FinishCall));

        EXPECT_EQ(client_puller.Pull(), grpc::CompletionQueue::GOT_EVENT);
        EXPECT_TRUE(client_puller.ok());
        EXPECT_THAT(client_puller.tag(), ::testing::AnyOf(Operation::WriteDone, Operation::FinishCall));

        ASSERT_PRED2(IsStatusEquals, clientCall_->responseStatus, grpc::Status());
        ASSERT_EQ(clientCall_->response_.num(), response_num);
    });
    auto server_done = std::async([&] {
        int readMessageCount = 0;
        StringMsg readMessage;
        serverCall_->ReadMessagesUntilOk(&readMessage, server_puller, readMessageCount, INT_MAX);
        EXPECT_EQ(readMessageCount, maxSentMessageCount);
        EXPECT_FALSE(server_puller.ok());
        // start finishing process
        serverCall_->FinishServerRpc(response_num);
        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::FinishCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
    });
    client_done.wait();
    server_done.wait();
}

TEST_F(ClientStreamFixture, DeadlineIfNoRunningServer) {
    using namespace std::chrono_literals;
    CreateClientStubWithWrongChannel();
    CompletionQueuePuller client_puller(client_->CompletionQueue());
    StartClientCall(std::chrono::system_clock::now() + 10ms);
    // Call is successful even server doesn't wait for the call
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, false,
                        grpc::CompletionQueue::GOT_EVENT);

    clientCall_->Finish();
    EXPECT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
    EXPECT_THAT(clientCall_->responseStatus.error_code(),
                ::testing::AnyOf(grpc::StatusCode::DEADLINE_EXCEEDED, grpc::StatusCode::UNAVAILABLE));
}

TEST_F(ClientStreamFixture, DeadlineIfServerNotServes) {
    using namespace std::chrono_literals;
    StartServer();
    ConnectClientStubToServer();
    CompletionQueuePuller client_puller(client_->CompletionQueue());
    StartClientCall(std::chrono::system_clock::now() + 10ms);
    // Call is successful even server doesn't wait for the call
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);
    int sentMessageCount = 0;
    clientCall_->SendMessagesUntilOk(client_puller, INT_MAX, sentMessageCount, 10ms);
    ASSERT_FALSE(client_puller.ok());

    clientCall_->Finish();
    EXPECT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
    EXPECT_EQ(clientCall_->responseStatus.error_code(), grpc::StatusCode::DEADLINE_EXCEEDED);

    // Even client terminates rpc, server don't know about
    CompletionQueuePuller server_puller(server_->CompletionQueue());
    StartServerWaiting();
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);
    StringMsg readMessage;
    serverCall_->Read(&readMessage);
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::ReadCall, false, grpc::CompletionQueue::GOT_EVENT);
    serverCall_->FinishServerRpc(1);
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::FinishCall, false,
                        grpc::CompletionQueue::GOT_EVENT);
}

class ClientStreamFixtureTimeout : public ClientStreamFixture, public ::testing::WithParamInterface<TimeoutScenario> {};

INSTANTIATE_TEST_SUITE_P(ScenarioArguments, ClientStreamFixtureTimeout,
                         testing::Values(TimeoutScenario::ExpiredBeforeRequest, TimeoutScenario::ExpiredBeforeAccept,
                                         TimeoutScenario::ExpiredBeforeWrite, TimeoutScenario::ExpiredBeforeRead,
                                         TimeoutScenario::ExpiredBeforeWritesDone,
                                         TimeoutScenario::ExpiredBeforeReadDone,
                                         TimeoutScenario::ExpiredBeforeServerFinish,
                                         TimeoutScenario::ExpiredBeforeClientReceivesFinish,
                                         TimeoutScenario::NotExpired));

TEST_P(ClientStreamFixtureTimeout, DeadlineWaitServerResponse) {
    using namespace std::chrono_literals;
    StartServer();
    ConnectClientStubToServer();
    CompletionQueuePuller server_puller(server_->CompletionQueue());
    CompletionQueuePuller client_puller(client_->CompletionQueue());
    auto start = std::chrono::system_clock::now();
    const auto context_timeout = 50ms;
    bool expired = false;
    if (GetParam() == TimeoutScenario::ExpiredBeforeRequest) {
        std::this_thread::sleep_until(start + 2 * context_timeout);
        expired = true;
    }
    // send request to server
    StartClientCall(start + context_timeout);
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, !expired,
                        grpc::CompletionQueue::GOT_EVENT);
    if (client_puller.ok()) {
        if (GetParam() == TimeoutScenario::ExpiredBeforeAccept) {
            std::this_thread::sleep_until(start + 2 * context_timeout);
            expired = true;
        }
        // server doesn't sensitive to deadline
        StartServerWaiting();
        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                            grpc::CompletionQueue::GOT_EVENT);

        if (GetParam() == TimeoutScenario::ExpiredBeforeWrite) {
            std::this_thread::sleep_until(start + 2 * context_timeout);
            expired = true;
        }

        StringMsg sentMessage;
        clientCall_->Write(sentMessage);
        EXPECT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::WriteCall, !expired,
                            grpc::CompletionQueue::GOT_EVENT);
        if (server_puller.ok()) {
            if (GetParam() == TimeoutScenario::ExpiredBeforeRead) {
                std::this_thread::sleep_until(start + 2 * context_timeout);
                expired = true;
            }
            StringMsg readMessage;
            serverCall_->Read(&readMessage);
            ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::ReadCall, !expired,
                                grpc::CompletionQueue::GOT_EVENT);
        }
        if (client_puller.ok()) {
            if (GetParam() == TimeoutScenario::ExpiredBeforeWritesDone) {
                std::this_thread::sleep_until(start + 2 * context_timeout);
                expired = true;
            }

            clientCall_->WritesDone();
            EXPECT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::WriteDone, !expired,
                                grpc::CompletionQueue::GOT_EVENT);
        }
        if (server_puller.ok()) {
            if (GetParam() == TimeoutScenario::ExpiredBeforeReadDone) {
                std::this_thread::sleep_until(start + 2 * context_timeout);
                expired = true;
            }
            StringMsg readMessage;
            serverCall_->Read(&readMessage);
            ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::ReadCall, false,
                                grpc::CompletionQueue::GOT_EVENT);
        }

        if (GetParam() == TimeoutScenario::ExpiredBeforeServerFinish) {
            std::this_thread::sleep_until(start + 2 * context_timeout);
            expired = true;
        }
        serverCall_->FinishServerRpc(12313);
        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::FinishCall, !expired,
                            grpc::CompletionQueue::GOT_EVENT);

        if (GetParam() == TimeoutScenario::ExpiredBeforeClientReceivesFinish) {
            std::this_thread::sleep_until(start + 2 * context_timeout);
            expired = true;
        }
        clientCall_->Finish();
        EXPECT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        if (expired) {
            EXPECT_EQ(clientCall_->responseStatus.error_code(), grpc::StatusCode::DEADLINE_EXCEEDED);
        } else {
            EXPECT_TRUE(clientCall_->responseStatus.ok());
        }
    }
}

TEST(WrongUsage, NeverLeftUnhandledRequestsInQueueAndDeleteClientWriter) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    ASSERT_DEATH(
        {
            auto client =
                std::make_unique<Client>(grpc::CreateChannel("localhost:111", grpc::InsecureChannelCredentials()));
            auto call = std::make_unique<ClientCall>(*client, std::nullopt);
            call.reset();  // there is call left in the queue
            client.reset();// crashes here
        },
        ".*");
}

TEST(WrongUsage, NeverLeftUnhandledRequestsInQueueAndDeleteServerReader) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    ASSERT_DEATH(
        {
            auto server = std::make_unique<Server>();
            auto serverCall = std::make_unique<ServerCall>(*server);
            serverCall.reset();// there is call left in the queue
            server.reset();    // crashes here
        },
        ".*");
}
