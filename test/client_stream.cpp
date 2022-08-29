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

struct Scenario {
    enum ScenarioMode { ServerStopsReadingAndSendsResponse, ClientWritesDoneAndServerSendsResponse };
    ScenarioMode scenario;
};

std::ostream &operator<<(std::ostream &os, const Scenario &value) {
    return os << magic_enum::enum_name(value.scenario);
}

std::ostream &operator<<(std::ostream &os, const google::protobuf::Message &value) { return os << value.DebugString(); }

class ClientStreamFixture : public ::testing::Test {
protected:
    enum Operation : std::intptr_t {// has to be the same size as pointer
        IncomingCall,
        OutgoingCall,
        WriteCall,
        ReadCall,
        FinishCall,
        WriteDone,
    };
    void SetUp() override {
        grpc::ServerBuilder builder;
        builder.AddListeningPort("[::]:", grpc::InsecureServerCredentials(), &selectedPort_);
        builder.RegisterService(&service_);
        serverCompletionQueue_ = builder.AddCompletionQueue();
        server_ = builder.BuildAndStart();
    }
    void ConnectClientStubToServer() {
        //auto in_process_channel = server_->InProcessChannel(grpc::ChannelArguments());
        //clientStub_ = MyService::NewStub(in_process_channel);
        auto serverChannel =
            grpc::CreateChannel("localhost:" + std::to_string(selectedPort_), grpc::InsecureChannelCredentials());
        clientStub_ = MyService::NewStub(serverChannel);
    }
    void CreateClientStubWithWrongChannel() {
        auto bad_channel =
            grpc::CreateChannel("localhost:" + std::to_string(selectedPort_ + 1), grpc::InsecureChannelCredentials());
        clientStub_ = MyService::NewStub(bad_channel);
    }
    void TearDown() override {
        server_->Shutdown();
        serverCompletionQueue_->Shutdown();
        DrainCompletionQueue(*serverCompletionQueue_);
        clientContext_.TryCancel();
        clientCompletionQueue_.Shutdown();
        DrainCompletionQueue(clientCompletionQueue_);
    }
    static void DrainCompletionQueue(grpc::CompletionQueue &cq) {
        void *tag;
        bool ok;
        while (cq.Next(&tag, &ok))
            ;
    }
    void StartServerWaiting(void *tag) {
        serverReader_ = std::make_unique<grpc::ServerAsyncReader<CountMsg, StringMsg>>(&serverContext_);
        service_.RequestClientStream(&serverContext_, serverReader_.get(), serverCompletionQueue_.get(),
                                     serverCompletionQueue_.get(), tag);
    }
    [[nodiscard]] std::unique_ptr<grpc::ClientAsyncWriter<StringMsg>> StartClientCall(CountMsg &response, void *tag) {
        return clientStub_->AsyncClientStream(&clientContext_, &response, &clientCompletionQueue_, tag);
    }
    class CompletionQueuePuller {
    private:
        grpc::CompletionQueue &cq_;
        bool ok_{};
        Operation tag_{};
        grpc::CompletionQueue::NextStatus status_{};

    public:
        explicit CompletionQueuePuller(grpc::CompletionQueue &cq) : cq_(cq) {}
        grpc::CompletionQueue::NextStatus Pull() {
            auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(100);
            status_ = cq_.AsyncNext((void **) &tag_, &ok_, deadline);
            return status_;
        }
        [[nodiscard]] bool ok() const { return ok_; }
        [[nodiscard]] Operation tag() const { return tag_; }
        [[nodiscard]] grpc::CompletionQueue::NextStatus status() const { return status_; }
    };
    static testing::AssertionResult AssertCompletion(const char *cq_str, const char *tag_str, const char *ok_str,
                                                     const char *next_str, CompletionQueuePuller &puller,
                                                     Operation expected_tag, bool expected_ok,
                                                     grpc::CompletionQueue::NextStatus expected_next) {
        auto next = puller.Pull();
        if (next == expected_next) {
            if (next != grpc::CompletionQueue::GOT_EVENT
                || (expected_tag == puller.tag() && expected_ok == puller.ok()))
                return testing::AssertionSuccess();
            auto failure = testing::AssertionFailure();
            failure << cq_str;
            if (expected_ok != puller.ok())
                failure << "\nok " << puller.ok() << " expected " << expected_ok;
            if (expected_tag != puller.tag())
                failure << "\ntag " << magic_enum::enum_name(puller.tag()) << " expected "
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
                                    int max_count, int &sentMessageCount) {
        while (sentMessageCount < max_count) {
            ++sentMessageCount;
            StringMsg sentMessage;
            sentMessage.set_text(std::to_string(sentMessageCount));
            writer.Write(sentMessage, reinterpret_cast<void *>(Operation::WriteCall));
            if (puller.Pull() != grpc::CompletionQueue::GOT_EVENT || !puller.ok())
                break;
            ASSERT_EQ(puller.tag(), Operation::WriteCall);
        }
    }

    void ReadMessagesUntilOk(StringMsg *readMessage, CompletionQueuePuller &puller, int &readMessageCount) {
        for (;;) {
            readMessage->set_text("not initialized");
            serverReader_->Read(readMessage, reinterpret_cast<void *>(Operation::ReadCall));
            if (puller.Pull() != grpc::CompletionQueue::GOT_EVENT || !puller.ok())
                break;
            ++readMessageCount;
            ASSERT_EQ(puller.tag(), Operation::ReadCall);
            ASSERT_EQ(std::to_string(readMessageCount), readMessage->text());
        }
    }
    static void MakeServerAndClientAreCompleted(CompletionQueuePuller &client_puller,
                                                grpc::ClientAsyncWriter<StringMsg> &writer,
                                                CompletionQueuePuller &server_puller,
                                                bool client_is_waiting_for_completion) {
        if (client_is_waiting_for_completion) {
            // Now client is ready to send the next message
            ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::WriteCall, true,
                                grpc::CompletionQueue::GOT_EVENT);
        }
        // server is waiting for the message, so send them it
        StringMsg sentMessage;
        writer.Write(sentMessage, reinterpret_cast<void *>(Operation::WriteCall));
        // complete both read and write calls
        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::ReadCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::WriteCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
    }
    void FinishServerRpc(int finish_num, CompletionQueuePuller& puller){
        grpc::Status okStatus;
        CountMsg serverResponse;
        serverResponse.set_num(finish_num);
        serverReader_->Finish(serverResponse, okStatus, reinterpret_cast<void *>(Operation::FinishCall));
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::FinishCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        serverReader_.reset();
    }
    void FinishServerRpcWithError(const grpc::Status& serverFinishStatus, CompletionQueuePuller& puller){
        serverReader_->FinishWithError(serverFinishStatus, reinterpret_cast<void *>(Operation::FinishCall));
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::FinishCall, true,
                                grpc::CompletionQueue::GOT_EVENT);
    }
protected:
    MyService::AsyncService service_;
    std::unique_ptr<grpc::Server> server_;
    std::unique_ptr<grpc::ServerCompletionQueue> serverCompletionQueue_;
    grpc::CompletionQueue clientCompletionQueue_;
    std::unique_ptr<MyService::Stub> clientStub_;
    grpc::ServerContext serverContext_;
    grpc::ClientContext clientContext_;
    std::unique_ptr<grpc::ServerAsyncReader<CountMsg, StringMsg>> serverReader_;
    int selectedPort_ = -1;
};
class ClientStreamFixtureWithParam: public ClientStreamFixture, public ::testing::WithParamInterface<Scenario>
{};

INSTANTIATE_TEST_SUITE_P(ScenarioArguments, ClientStreamFixtureWithParam,
                         testing::Values(Scenario{Scenario::ClientWritesDoneAndServerSendsResponse},
                                         Scenario{Scenario::ServerStopsReadingAndSendsResponse}
                                         ));
TEST_F(ClientStreamFixture, CheckNoGrpcByteStreamAssert) {
    // In version 1.45 there was introduced a bug with assert
    // byte_stream.cc:63 assertion failed: backing_buffer_.count > 0
    ConnectClientStubToServer();
    CompletionQueuePuller server_puller(*serverCompletionQueue_);
    CompletionQueuePuller client_puller(clientCompletionQueue_);

    CountMsg response;
    auto writer = StartClientCall(response, reinterpret_cast<void *>(Operation::OutgoingCall));
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);

    StartServerWaiting(reinterpret_cast<void *>(Operation::IncomingCall));
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);
    StringMsg readMessage;
    serverReader_->Read(&readMessage, reinterpret_cast<void *>(Operation::ReadCall));
    StringMsg sentMessage;
    writer->Write(sentMessage, reinterpret_cast<void *>(Operation::WriteCall));
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::WriteCall, true,
                        grpc::CompletionQueue::GOT_EVENT);
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::ReadCall, true,
                        grpc::CompletionQueue::GOT_EVENT);

    const int finish_num = 2124146;// random number
    FinishServerRpc(finish_num, server_puller);

    int sentMessageCount = 0;
    SendMessagesUntilOk(*writer, client_puller, INT_MAX, sentMessageCount);
}

TEST_P(ClientStreamFixtureWithParam, CheckScenario) {
    ConnectClientStubToServer();
    CompletionQueuePuller server_puller(*serverCompletionQueue_);
    CompletionQueuePuller client_puller(clientCompletionQueue_);

    const int no_response_num = -1;
    CountMsg response;
    response.set_num(no_response_num);

    // Client gets writer
    auto writer = StartClientCall(response, reinterpret_cast<void *>(Operation::OutgoingCall));
    // Call is successful even server doesn't wait for the call
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);

    const int maxSentMessageCount = 100;
    int sentMessageCount = 0;
    SendMessagesUntilOk(*writer, client_puller, maxSentMessageCount, sentMessageCount);
    if (client_puller.Pull() == grpc::CompletionQueue::GOT_EVENT) {
        ASSERT_TRUE(client_puller.ok());
    } else {
        ASSERT_EQ(client_puller.status(), grpc::CompletionQueue::TIMEOUT);
    }

    // Server makes themselves ready for getting the call
    StartServerWaiting(reinterpret_cast<void *>(Operation::IncomingCall));
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);

    // Server got the call and ready for reading
    int readMessageCount = 0;
    StringMsg readMessage;
    ReadMessagesUntilOk(&readMessage, server_puller, readMessageCount);
    // server read all messages sent by client
    ASSERT_EQ(readMessageCount, sentMessageCount);
    // server is waiting for the next message
    ASSERT_EQ(server_puller.status(), grpc::CompletionQueue::TIMEOUT);

    MakeServerAndClientAreCompleted(client_puller, *writer, server_puller, sentMessageCount < maxSentMessageCount);
    if (GetParam().scenario == Scenario::ServerStopsReadingAndSendsResponse) {
        const int finish_num = 2124146;// random number
        FinishServerRpc(finish_num, server_puller);
        // client doesn't know about it and goes on sending
        SendMessagesUntilOk(*writer, client_puller, INT_MAX, sentMessageCount);
        // client gets ok=false that is mean that server finishes rpc
        ASSERT_EQ(client_puller.status(), grpc::CompletionQueue::GOT_EVENT);
        ASSERT_FALSE(client_puller.ok());
        // to know the status client has to finish rpc from their side
        grpc::Status responseStatus(grpc::INTERNAL, "not initialized");
        writer->Finish(&responseStatus, reinterpret_cast<void *>(Operation::FinishCall));
        ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        ASSERT_PRED2(IsStatusEquals, responseStatus, grpc::Status());
        ASSERT_EQ(response.num(), finish_num);
    } else if (GetParam().scenario == Scenario::ClientWritesDoneAndServerSendsResponse) {
        // client writes done
        writer->WritesDone(reinterpret_cast<void *>(Operation::WriteDone));
        ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::WriteDone, true,
                            grpc::CompletionQueue::GOT_EVENT);
        grpc::Status responseStatus(grpc::INTERNAL, "not initialized");
        writer->Finish(&responseStatus, reinterpret_cast<void *>(Operation::FinishCall));
        // client is waiting for server response, so, timeout
        ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true,
                            grpc::CompletionQueue::TIMEOUT);
        // server gets false and sends response
        serverReader_->Read(&readMessage, reinterpret_cast<void *>(Operation::ReadCall));
        ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::ReadCall, false,
                            grpc::CompletionQueue::GOT_EVENT);
        // start finishing process
        const int finish_num = 2124146;// random number
        FinishServerRpc(finish_num, server_puller);
        // client gets server response
        ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        ASSERT_PRED2(IsStatusEquals, responseStatus, grpc::Status());
        ASSERT_EQ(response.num(), finish_num);
    }
}
