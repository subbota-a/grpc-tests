#include <chrono>
#include <future>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <magic_enum.hpp>
#include <memory>
#include <test.grpc.pb.h>
#include <test.pb.h>

using mypkg::CountMsg;
using mypkg::MyService;
using mypkg::StringMsg;
using namespace magic_enum::ostream_operators;

namespace google::protobuf {
std::ostream &operator<<(std::ostream &os, const Message &value) { return os << value.DebugString(); }
}// namespace google::protobuf
namespace grpc {
std::ostream &operator<<(std::ostream &os, CompletionQueue::NextStatus value) {
    return os << magic_enum::enum_name(value);
}
std::ostream &operator<<(std::ostream &os, StatusCode value) {
    return os << magic_enum::enum_name(value);
}

}// namespace grpc
enum Operation : std::intptr_t {// has to be the same size as pointer
    IncomingCall,
    OutgoingCall,
    WriteCall,
    ReadCall,
    FinishCall,
    WriteDone,
};

class CompletionQueuePuller {
private:
    grpc::CompletionQueue &cq_;
    bool ok_{};
    Operation tag_{};
    grpc::CompletionQueue::NextStatus status_{};
    std::chrono::milliseconds timeout_;

public:
    explicit CompletionQueuePuller(grpc::CompletionQueue &cq,
                                   std::chrono::milliseconds timeout = std::chrono::milliseconds(100))
        : cq_(cq), timeout_(timeout) {}
    grpc::CompletionQueue::NextStatus Pull() {
        auto deadline = std::chrono::system_clock::now() + timeout_;
        status_ = cq_.AsyncNext((void **) &tag_, &ok_, deadline);
        return status_;
    }
    [[nodiscard]] bool ok() const { return ok_; }
    [[nodiscard]] Operation tag() const { return tag_; }
    [[nodiscard]] grpc::CompletionQueue::NextStatus status() const { return status_; }
};

static void DrainCompletionQueue(grpc::CompletionQueue &cq) {
    void *tag;
    bool ok;
    while (cq.Next(&tag, &ok))
        ;
}

class Server{
public:
    Server(){
        grpc::ServerBuilder builder;
        builder.AddListeningPort("localhost:8888", grpc::InsecureServerCredentials(), &selectedPort_);
        builder.RegisterService(&service_);
        serverCompletionQueue_ = builder.AddCompletionQueue();
        server_ = builder.BuildAndStart();
    }
    ~Server(){
        if (server_)
            server_->Shutdown();
        serverCompletionQueue_->Shutdown();
        DrainCompletionQueue(*serverCompletionQueue_);
        if (server_)
            server_->Wait();
    }
    [[nodiscard]] int Port() const { return selectedPort_; }
    [[nodiscard]] grpc::CompletionQueue& CompletionQueue() { return *serverCompletionQueue_; }
    void RequestClientStream(grpc::ServerContext* context, grpc::ServerAsyncReader<CountMsg, StringMsg>* reader, void* tag)
    {
        service_.RequestClientStream(context, reader, serverCompletionQueue_.get(),
                                     serverCompletionQueue_.get(), tag);
    }
private:
    int selectedPort_ = -1;
    MyService::AsyncService service_;
    std::unique_ptr<grpc::Server> server_;
    std::unique_ptr<grpc::ServerCompletionQueue> serverCompletionQueue_;
};

class BaseFixture: public ::testing::Test {
};

class ClientStreamFixture : public BaseFixture {
protected:
    void SetUp() override {
    }
    void StartServer() { server_ = std::make_unique<Server>(); }
    void ConnectClientStubToServer() {
        ASSERT_TRUE(server_);
        ASSERT_GT(server_->Port(), 0);
        auto serverChannel =
            grpc::CreateChannel("localhost:" + std::to_string(server_->Port()), grpc::InsecureChannelCredentials());
        clientStub_ = MyService::NewStub(serverChannel);
    }
    void CreateClientStubWithWrongChannel() {
        auto bad_channel = grpc::CreateChannel("localhost:888", grpc::InsecureChannelCredentials());
        clientStub_ = MyService::NewStub(bad_channel);
    }
    void TearDown() override {
        server_.reset();
        serverReader_.reset();
        clientStub_.reset();
        clientContext_.TryCancel();
        clientCompletionQueue_.Shutdown();
        DrainCompletionQueue(clientCompletionQueue_);
    }
    void StartServerWaiting(void *tag) {
        serverReader_ = std::make_unique<grpc::ServerAsyncReader<CountMsg, StringMsg>>(&serverContext_);
        server_->RequestClientStream(&serverContext_, serverReader_.get(), tag);
    }
    [[nodiscard]] std::unique_ptr<grpc::ClientAsyncWriter<StringMsg>> StartClientCall(CountMsg *response, void *tag) {
        return clientStub_->AsyncClientStream(&clientContext_, response, &clientCompletionQueue_, tag);
    }
    static testing::AssertionResult
    AssertCompletion(const char *cq_str, [[maybe_unused]] const char *tag_str, [[maybe_unused]] const char *ok_str,
                     [[maybe_unused]] const char *next_str, CompletionQueuePuller &puller, Operation expected_tag,
                     bool expected_ok, grpc::CompletionQueue::NextStatus expected_next) {
        auto next = puller.Pull();
        if (next == expected_next) {
            if (next != grpc::CompletionQueue::GOT_EVENT
                || (expected_tag == puller.tag() && expected_ok == puller.ok()))
                return testing::AssertionSuccess();
            auto failure = testing::AssertionFailure();
            failure << cq_str;
            if (expected_ok != puller.ok())
                failure << "\nok: actual " << puller.ok() << " expected " << expected_ok;
            if (expected_tag != puller.tag())
                failure << "\ntag: actual " << magic_enum::enum_name(puller.tag()) << " expected "
                        << magic_enum::enum_name(expected_tag);
            return failure;
        } else {
            return testing::AssertionFailure() << cq_str << " returns " << magic_enum::enum_name(next) << " expected "
                                               << magic_enum::enum_name(expected_next);
        }
    }
    static bool IsStatusEquals(const grpc::Status &stat1, const grpc::Status &stat2) {
        return stat1.error_code() == stat2.error_code() && stat1.error_message() == stat2.error_message();
    }

    static void SendMessagesUntilOk(grpc::ClientAsyncWriter<StringMsg> &writer, CompletionQueuePuller &puller,
                                    int max_count, int &sentMessageCount, std::optional<std::chrono::milliseconds> delay = {}) {
        while (sentMessageCount < max_count) {
            ++sentMessageCount;
            StringMsg sentMessage;
            sentMessage.set_text(std::to_string(sentMessageCount));
            auto opt = grpc::WriteOptions();
            if (sentMessageCount + 1 < max_count)
                opt.set_buffer_hint();
            writer.Write(sentMessage, opt, reinterpret_cast<void *>(Operation::WriteCall));
            if (puller.Pull() != grpc::CompletionQueue::GOT_EVENT || !puller.ok())
                break;
            ASSERT_EQ(puller.tag(), Operation::WriteCall);
            if (delay){
                std::this_thread::sleep_for(*delay);
            }
        }
    }

    void ReadMessagesUntilOk(StringMsg *readMessage, CompletionQueuePuller &puller, int &readMessageCount,
                             int maxReadCount) {
        for (readMessageCount = 0; readMessageCount < maxReadCount;) {
            readMessage->set_text("not initialized");
            serverReader_->Read(readMessage, reinterpret_cast<void *>(Operation::ReadCall));
            if (puller.Pull() != grpc::CompletionQueue::GOT_EVENT || !puller.ok())
                break;
            ++readMessageCount;
            ASSERT_EQ(puller.tag(), Operation::ReadCall);
            ASSERT_EQ(std::to_string(readMessageCount), readMessage->text());
        }
    }
    void FinishServerRpc(int finish_num, CompletionQueuePuller &puller) {
        grpc::Status okStatus;
        CountMsg serverResponse;
        serverResponse.set_num(finish_num);
        serverReader_->Finish(serverResponse, okStatus, reinterpret_cast<void *>(Operation::FinishCall));
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
        serverReader_.reset();
    }
    void FinishServerRpcWithError(const grpc::Status &serverFinishStatus, CompletionQueuePuller &puller) {
        serverReader_->FinishWithError(serverFinishStatus, reinterpret_cast<void *>(Operation::FinishCall));
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
    }

protected:
    std::unique_ptr<Server> server_;
    grpc::CompletionQueue clientCompletionQueue_;
    std::unique_ptr<MyService::Stub> clientStub_;
    grpc::ServerContext serverContext_;
    grpc::ClientContext clientContext_;
    std::unique_ptr<grpc::ServerAsyncReader<CountMsg, StringMsg>> serverReader_;
};

TEST_F(ClientStreamFixture, IdealScenario) {
    StartServer();
    ConnectClientStubToServer();
    CompletionQueuePuller server_puller(server_->CompletionQueue());
    CompletionQueuePuller client_puller(clientCompletionQueue_);

    const int no_response_num = -1;
    const int response_num = 2124146;// random number
    CountMsg response;
    response.set_num(no_response_num);

    // Client gets writer
    auto writer = StartClientCall(&response, reinterpret_cast<void *>(Operation::OutgoingCall));
    // Call is successful even server doesn't wait for the call
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);

    // Server makes themselves ready for getting the call
    StartServerWaiting(reinterpret_cast<void *>(Operation::IncomingCall));
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);

    const int maxSentMessageCount = 1000;
    auto client_done = std::async([&] {
        int sentMessageCount = 0;
        SendMessagesUntilOk(*writer, client_puller, maxSentMessageCount, sentMessageCount);
        EXPECT_EQ(maxSentMessageCount, sentMessageCount);

        writer->WritesDone(reinterpret_cast<void *>(Operation::WriteDone));
        EXPECT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::WriteDone, true,
                            grpc::CompletionQueue::GOT_EVENT);

        grpc::Status responseStatus(grpc::INTERNAL, "not initialized");
        writer->Finish(&responseStatus, reinterpret_cast<void *>(Operation::FinishCall));
        EXPECT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        ASSERT_PRED2(IsStatusEquals, responseStatus, grpc::Status());
        ASSERT_EQ(response.num(), response_num);
    });
    auto server_done = std::async([&] {
        int readMessageCount = 0;
        StringMsg readMessage;
        ReadMessagesUntilOk(&readMessage, server_puller, readMessageCount, INT_MAX);
        EXPECT_EQ(readMessageCount, maxSentMessageCount);
        EXPECT_FALSE(server_puller.ok());
        // start finishing process
        FinishServerRpc(response_num, server_puller);
    });
    client_done.wait();
    server_done.wait();
}

TEST_F(ClientStreamFixture, DeadlineIfNoRunningServer) {
    using namespace std::chrono_literals;
    CompletionQueuePuller client_puller(clientCompletionQueue_);
    CreateClientStubWithWrongChannel();
    clientContext_.set_deadline(std::chrono::system_clock::now() + 10ms);
    CountMsg response;
    auto writer = StartClientCall(&response, reinterpret_cast<void *>(Operation::OutgoingCall));
    // Call is successful even server doesn't wait for the call
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, false,
                        grpc::CompletionQueue::GOT_EVENT);

    grpc::Status responseStatus;
    writer->Finish(&responseStatus, reinterpret_cast<void *>(Operation::FinishCall));
    EXPECT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
    EXPECT_TRUE(responseStatus.error_code()==grpc::StatusCode::DEADLINE_EXCEEDED || responseStatus.error_code()==grpc::StatusCode::UNAVAILABLE);
}

TEST_F(ClientStreamFixture, DeadlineIfServerNotServes) {
    using namespace std::chrono_literals;
    CompletionQueuePuller client_puller(clientCompletionQueue_);
    StartServer();
    ConnectClientStubToServer();
    clientContext_.set_deadline(std::chrono::system_clock::now() + 10ms);
    CountMsg response;
    auto writer = StartClientCall(&response, reinterpret_cast<void *>(Operation::OutgoingCall));
    // Call is successful even server doesn't wait for the call
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);
    int sentMessageCount = 0;
    SendMessagesUntilOk(*writer, client_puller, INT_MAX, sentMessageCount, 10ms);
    ASSERT_FALSE(client_puller.ok());

    grpc::Status responseStatus;
    writer->Finish(&responseStatus, reinterpret_cast<void *>(Operation::FinishCall));
    EXPECT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
    EXPECT_EQ(responseStatus.error_code(), grpc::StatusCode::DEADLINE_EXCEEDED);

    // Even client terminates rpc, server don't know about
    CompletionQueuePuller server_puller(server_->CompletionQueue());
    StartServerWaiting(reinterpret_cast<void *>(Operation::IncomingCall));
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);
    StringMsg readMessage;
    serverReader_->Read(&readMessage, reinterpret_cast<void *>(Operation::ReadCall));
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::ReadCall, false, grpc::CompletionQueue::GOT_EVENT);
    grpc::Status okStatus;
    CountMsg serverResponse;
    serverResponse.set_num(1);
    serverReader_->Finish(serverResponse, okStatus, reinterpret_cast<void *>(Operation::FinishCall));
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::FinishCall, false,
                        grpc::CompletionQueue::GOT_EVENT);
}

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
    CompletionQueuePuller client_puller(clientCompletionQueue_);
    CompletionQueuePuller server_puller(server_->CompletionQueue());
    ConnectClientStubToServer();
    auto start = std::chrono::system_clock::now();
    const auto context_timeout = 50ms;
    clientContext_.set_deadline(start + context_timeout);
    CountMsg response;
    bool expired = false;
    if (GetParam() == TimeoutScenario::ExpiredBeforeRequest) {
        std::this_thread::sleep_until(start + 2 * context_timeout);
        expired = true;
    }
    // send request to server
    auto writer = StartClientCall(&response, reinterpret_cast<void *>(Operation::OutgoingCall));
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, !expired,
                        grpc::CompletionQueue::GOT_EVENT);
    if (client_puller.ok()) {
        if (GetParam() == TimeoutScenario::ExpiredBeforeAccept) {
            std::this_thread::sleep_until(start + 2 * context_timeout);
            expired = true;
        }
        // server doesn't sensitive to deadline
        StartServerWaiting(reinterpret_cast<void *>(Operation::IncomingCall));
        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                            grpc::CompletionQueue::GOT_EVENT);

        if (GetParam() == TimeoutScenario::ExpiredBeforeWrite) {
            std::this_thread::sleep_until(start + 2 * context_timeout);
            expired = true;
        }

        StringMsg sentMessage;
        writer->Write(sentMessage, reinterpret_cast<void *>(Operation::WriteCall));
        EXPECT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::WriteCall, !expired,
                            grpc::CompletionQueue::GOT_EVENT);
        if (server_puller.ok()) {
            if (GetParam() == TimeoutScenario::ExpiredBeforeRead) {
                std::this_thread::sleep_until(start + 2 * context_timeout);
                expired = true;
            }
            StringMsg readMessage;
            serverReader_->Read(&readMessage, reinterpret_cast<void *>(Operation::ReadCall));
            ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::ReadCall, !expired,
                                grpc::CompletionQueue::GOT_EVENT);
        }
        if (client_puller.ok()) {
            if (GetParam() == TimeoutScenario::ExpiredBeforeWritesDone) {
                std::this_thread::sleep_until(start + 2 * context_timeout);
                expired = true;
            }

            writer->WritesDone(reinterpret_cast<void *>(Operation::WriteDone));
            EXPECT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::WriteDone, !expired,
                                grpc::CompletionQueue::GOT_EVENT);
        }
        if (server_puller.ok()) {
            if (GetParam() == TimeoutScenario::ExpiredBeforeReadDone) {
                std::this_thread::sleep_until(start + 2 * context_timeout);
                expired = true;
            }
            StringMsg readMessage;
            serverReader_->Read(&readMessage, reinterpret_cast<void *>(Operation::ReadCall));
            ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::ReadCall, false,
                                grpc::CompletionQueue::GOT_EVENT);
        }

        if (GetParam() == TimeoutScenario::ExpiredBeforeServerFinish) {
            std::this_thread::sleep_until(start + 2 * context_timeout);
            expired = true;
        }
        CountMsg server_response;
        serverReader_->Finish(server_response, grpc::Status(), reinterpret_cast<void *>(Operation::FinishCall));
        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::FinishCall, !expired,
                            grpc::CompletionQueue::GOT_EVENT);

        if (GetParam() == TimeoutScenario::ExpiredBeforeClientReceivesFinish) {
            std::this_thread::sleep_until(start + 2 * context_timeout);
            expired = true;
        }
        grpc::Status responseStatus;
        writer->Finish(&responseStatus, reinterpret_cast<void *>(Operation::FinishCall));
        EXPECT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        if (expired) {
            EXPECT_EQ(responseStatus.error_code(), grpc::StatusCode::DEADLINE_EXCEEDED);
        } else {
            EXPECT_TRUE(responseStatus.ok());
        }
    }
}

TEST(ClientStreamDead, NeverLeftUnhandledRequestsInQueueAndDeleteWriter) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    using namespace std::chrono_literals;
    {
        auto channel =
            grpc::CreateChannel("localhost:111", grpc::InsecureChannelCredentials());
        auto client_stub = MyService::NewStub(channel);
        grpc::CompletionQueue cq;
        CompletionQueuePuller puller(cq);
        CountMsg msg;
        {
            {
                grpc::ClientContext client_context;
                grpc::Status retStatus;
                auto writer = client_stub->AsyncClientStream(&client_context, &msg, &cq,
                                                             reinterpret_cast<void *>(Operation::OutgoingCall));
            }
            cq.Shutdown();
            ASSERT_DEATH(DrainCompletionQueue(cq), "");
        }
    }
}

//
//    if (GetParam().scenario == Scenario::ServerStopsReadingAndSendsResponse) {
//        const int finish_num = 2124146;// random number
//        FinishServerRpc(finish_num, server_puller);
//        // client doesn't know about it and goes on sending
//        SendMessagesUntilOk(*writer, client_puller, INT_MAX, sentMessageCount);
//        // client gets ok=false that is mean that server finishes rpc
//        ASSERT_EQ(client_puller.status(), grpc::CompletionQueue::GOT_EVENT);
//        ASSERT_FALSE(client_puller.ok());
//        // to know the status client has to finish rpc from their side
//        grpc::Status responseStatus(grpc::INTERNAL, "not initialized");
//        writer->Finish(&responseStatus, reinterpret_cast<void *>(Operation::FinishCall));
//        ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true,
//                            grpc::CompletionQueue::GOT_EVENT);
//        ASSERT_PRED2(IsStatusEquals, responseStatus, grpc::Status());
//        ASSERT_EQ(response.num(), finish_num);
//    } else if (GetParam().scenario == Scenario::ClientWritesDoneAndServerSendsResponse) {
//    } else if (GetParam().scenario == Scenario::ClientFinishesAndServerSendsResponse) {
//        // client finishes without done
//        grpc::Status responseStatus(grpc::INTERNAL, "not initialized");
//        writer->Finish(&responseStatus, reinterpret_cast<void *>(Operation::FinishCall));
//        // client is waiting for server response, so, timeout
//        ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true,
//                            grpc::CompletionQueue::TIMEOUT);
//        // server gets doesn't got write done, so false and sends response
//        serverReader_->Read(&readMessage, reinterpret_cast<void *>(Operation::ReadCall));
//        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::ReadCall, false,
//                            grpc::CompletionQueue::GOT_EVENT);
//        // start finishing process
//        const int finish_num = 2124146;// random number
//        FinishServerRpc(finish_num, server_puller);
//        // client gets server response
//        ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true,
//                            grpc::CompletionQueue::GOT_EVENT);
//        ASSERT_PRED2(IsStatusEquals, responseStatus, grpc::Status());
//        ASSERT_EQ(response.num(), finish_num);
//    }
//}
