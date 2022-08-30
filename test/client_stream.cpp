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
namespace {

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
        auto serverChannel =
            grpc::CreateChannel("localhost:" + std::to_string(selectedPort_), grpc::InsecureChannelCredentials());
        clientStub_ = MyService::NewStub(serverChannel);
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

    void FinishServerRpc(int finish_num, CompletionQueuePuller &puller) {
        grpc::Status okStatus;
        CountMsg serverResponse;
        serverResponse.set_num(finish_num);
        serverReader_->Finish(serverResponse, okStatus, reinterpret_cast<void *>(Operation::FinishCall));
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
        serverReader_.reset();
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

TEST_F(ClientStreamFixture, CheckNoGrpcByteStreamAssert) {
    // In version 1.45 there was introduced a bug
    // Debug version fails here: byte_stream.cc:62] assertion failed: backing_buffer_.count > 0
    // Release version fails here: slice_buffer.cc:384] assertion failed: sb->count > 0
    ConnectClientStubToServer();
    CompletionQueuePuller server_puller(*serverCompletionQueue_);
    CompletionQueuePuller client_puller(clientCompletionQueue_);

    // start stream from client to server and wait for completion
    CountMsg response;
    auto writer = StartClientCall(response, reinterpret_cast<void *>(Operation::OutgoingCall));
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);

    // start waiting for client stream and wait for completion
    StartServerWaiting(reinterpret_cast<void *>(Operation::IncomingCall));
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);
    // start reading the message
    StringMsg readMessage;
    serverReader_->Read(&readMessage, reinterpret_cast<void *>(Operation::ReadCall));
    // start writing the message
    StringMsg sentMessage;
    writer->Write(sentMessage, reinterpret_cast<void *>(Operation::WriteCall));
    // wait for write completed
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::WriteCall, true, grpc::CompletionQueue::GOT_EVENT);
    // wait for read completed
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::ReadCall, true, grpc::CompletionQueue::GOT_EVENT);

    // finish server stream
    const int finish_num = 2124146;// random number
    FinishServerRpc(finish_num, server_puller);

    // client doesn't know about it and goes on writing until ok is true
    // here gRPC fails on assert!
    int sentMessageCount = 0;
    SendMessagesUntilOk(*writer, client_puller, INT_MAX, sentMessageCount);
}

}
