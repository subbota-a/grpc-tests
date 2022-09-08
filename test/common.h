#pragma once
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>
#include <iostream>
#include <magic_enum.hpp>
#include <test.grpc.pb.h>
#include <test.pb.h>

namespace google::protobuf {
inline std::ostream &operator<<(std::ostream &os, const Message &value) { return os << value.DebugString(); }
}// namespace google::protobuf

namespace grpc {

inline std::ostream &operator<<(std::ostream &os, CompletionQueue::NextStatus value) {
    return os << magic_enum::enum_name(value);
}

inline std::ostream &operator<<(std::ostream &os, StatusCode value) { return os << magic_enum::enum_name(value); }

}// namespace grpc

enum Operation : std::intptr_t {// has to be the same size as pointer
    IncomingCall,
    OutgoingCall,
    WriteCall,
    ReadCall,
    FinishCall,
    WriteDone,
    AsyncDone,
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

void DrainCompletionQueue(grpc::CompletionQueue &cq);
testing::AssertionResult AssertCompletion(const char *cq_str, [[maybe_unused]] const char *tag_str,
                                          [[maybe_unused]] const char *ok_str, [[maybe_unused]] const char *next_str,
                                          CompletionQueuePuller &puller, Operation expected_tag, bool expected_ok,
                                          grpc::CompletionQueue::NextStatus expected_next);
bool IsStatusEquals(const grpc::Status &stat1, const grpc::Status &stat2);
void ReadMessagesUntilOk(grpc::internal::AsyncReaderInterface<mypkg::StringMsg> &reader, mypkg::StringMsg *readMessage,
                         CompletionQueuePuller &puller, int &readMessageCount, int maxReadCount);
void SendMessagesUntilOk(grpc::internal::AsyncWriterInterface<mypkg::StringMsg>& writer, CompletionQueuePuller &puller, int max_count, int &sentMessageCount,
                         std::optional<std::chrono::milliseconds> delay);

class Server {
public:
    Server() {
        grpc::ServerBuilder builder;
        builder.AddListeningPort("[::]:8888", grpc::InsecureServerCredentials(), &selectedPort_);
        builder.RegisterService(&service_);
        serverCompletionQueue_ = builder.AddCompletionQueue();
        server_ = builder.BuildAndStart();
    }
    ~Server() {
        server_->Shutdown();
        serverCompletionQueue_->Shutdown();
        DrainCompletionQueue(*serverCompletionQueue_);
        server_->Wait();
    }
    [[nodiscard]] int Port() const { return selectedPort_; }
    [[nodiscard]] grpc::CompletionQueue &CompletionQueue() { return *serverCompletionQueue_; }
    void RequestClientStream(grpc::ServerContext *context,
                             grpc::ServerAsyncReader<mypkg::CountMsg, mypkg::StringMsg> *reader, void *tag) {
        service_.RequestClientStream(context, reader, serverCompletionQueue_.get(), serverCompletionQueue_.get(), tag);
    }
    void RequestServerStream(grpc::ServerContext *context, mypkg::CountMsg *request,
                             grpc::ServerAsyncWriter<mypkg::StringMsg> *writer, void *tag) {
        service_.RequestServerStream(context, request, writer, serverCompletionQueue_.get(),
                                     serverCompletionQueue_.get(), tag);
    }
    void RequestBiStream(grpc::ServerContext *context, grpc::ServerAsyncReaderWriter<mypkg::StringMsg, mypkg::StringMsg> *stream, void*tag){
        service_.RequestBiStream(context, stream, serverCompletionQueue_.get(),
                                 serverCompletionQueue_.get(), tag);
    }

private:
    int selectedPort_ = -1;
    mypkg::MyService::AsyncService service_;
    std::unique_ptr<grpc::Server> server_;
    std::unique_ptr<grpc::ServerCompletionQueue> serverCompletionQueue_;
};

class Client final {
public:
    explicit Client(std::shared_ptr<grpc::Channel> channel)
        : clientStub_(mypkg::MyService::NewStub(std::move(channel))) {}
    [[nodiscard]] grpc::CompletionQueue &CompletionQueue() { return completion_queue_; }

    ~Client() {
        clientStub_.reset();
        completion_queue_.Shutdown();
        DrainCompletionQueue(completion_queue_);
    }
    std::unique_ptr<grpc::ClientAsyncWriter<mypkg::StringMsg>> AsyncClientStream(grpc::ClientContext *context,
                                                                                 mypkg::CountMsg *response, void *tag) {
        return clientStub_->AsyncClientStream(context, response, &completion_queue_, tag);
    }
    std::unique_ptr<grpc::ClientAsyncReader<mypkg::StringMsg>>
    AsyncServerStream(grpc::ClientContext *context, const mypkg::CountMsg &request, void *tag) {
        return clientStub_->AsyncServerStream(context, request, &completion_queue_, tag);
    }
    std::unique_ptr<grpc::ClientAsyncReaderWriter<mypkg::StringMsg, mypkg::StringMsg>> AsyncBiStream(grpc::ClientContext *context, void *tag){
        return clientStub_->AsyncBiStream(context, &completion_queue_, tag);
    }

private:
    std::unique_ptr<mypkg::MyService::Stub> clientStub_;
    grpc::CompletionQueue completion_queue_;
};

class BaseFixture : public ::testing::Test {
protected:
    void StartServer() { server_ = std::make_unique<Server>(); }
    void ConnectClientStubToServer() {
        ASSERT_TRUE(server_);
        ASSERT_GT(server_->Port(), 0);
        auto serverChannel =
            grpc::CreateChannel("localhost:" + std::to_string(server_->Port()), grpc::InsecureChannelCredentials());
        client_ = std::make_unique<Client>(std::move(serverChannel));
    }
    void CreateClientStubWithWrongChannel() {
        auto bad_channel = grpc::CreateChannel("localhost:888", grpc::InsecureChannelCredentials());
        client_ = std::make_unique<Client>(std::move(bad_channel));
    }
    void TearDown() override {
        server_.reset();
        client_.reset();
    }

protected:
    std::unique_ptr<Server> server_;
    std::unique_ptr<Client> client_;
};
