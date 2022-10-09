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
class ClientCall final {
    grpc::ClientContext context_;
    std::unique_ptr<grpc::ClientAsyncReaderWriter<StringMsg, StringMsg>> stream_;

public:
    grpc::Status finish_status_;
    ClientCall(Client &client, std::optional<std::chrono::system_clock::time_point> deadline) {
        if (deadline)
            context_.set_deadline(*deadline);
        stream_ = client.AsyncBiStream(&context_, reinterpret_cast<void *>(Operation::OutgoingCall));
    }
    void TryCancel() { context_.TryCancel(); }

    void Read(StringMsg *msg) { stream_->Read(msg, reinterpret_cast<void *>(Operation::ReadCall)); }
    void Write(const StringMsg &msg) { stream_->Write(msg, reinterpret_cast<void *>(Operation::WriteCall)); }
    void WritesDone() { stream_->WritesDone(reinterpret_cast<void *>(Operation::WriteDone)); }
    void Finish() { stream_->Finish(&finish_status_, reinterpret_cast<void *>(Operation::FinishCall)); }
};

class ServerCall final {
    grpc::ServerContext context_;
    grpc::ServerAsyncReaderWriter<StringMsg, StringMsg> stream_;

public:
    explicit ServerCall(Server &server) : stream_(&context_) {
        server.RequestBiStream(&context_, &stream_, reinterpret_cast<void *>(Operation::IncomingCall));
    }
    void Write(const StringMsg &msg) { stream_.Write(msg, reinterpret_cast<void *>(Operation::WriteCall)); }
    void Read(StringMsg *msg) { stream_.Read(msg, reinterpret_cast<void *>(Operation::ReadCall)); }
    void Finish(grpc::Status finish_status) {
        stream_.Finish(finish_status, reinterpret_cast<void *>(Operation::FinishCall));
    }
};
}// namespace

class BidiStreamFixture : public BaseFixture {
protected:
    [[nodiscard]] std::unique_ptr<ServerCall> StartServerStream() {
        return std::make_unique<ServerCall>(*server_);
    }
    [[nodiscard]] std::unique_ptr<ClientCall>
    StartClientCall(std::optional<std::chrono::system_clock::time_point> deadline = {}) {
        return std::make_unique<ClientCall>(*client_, deadline);
    }
};

TEST_F(BidiStreamFixture, IdealScenario) {
    using namespace std::chrono_literals;
    StartServer();
    ConnectClientStubToServer();
    const int requested_count = 1000;
    auto core = [&](auto call, CompletionQueuePuller& puller, const char* write_text, const char* read_text){
        StringMsg write_msg;
        StringMsg read_msg;
        write_msg.set_text(write_text);
        int write_count = 0, write_completion_count = 0;
        int read_count = 0, read_completion_count = 0;
        bool read_finished = false;
        while(write_completion_count < requested_count || !read_finished){
            if (write_count>write_completion_count || read_count>read_completion_count){
                puller.Pull();
                ASSERT_EQ(puller.status(), grpc::CompletionQueue::NextStatus::GOT_EVENT);
                if (puller.tag() == Operation::WriteCall) {
                    ++write_completion_count;
                    ASSERT_EQ(write_count,write_completion_count);
                    ASSERT_TRUE(puller.ok());
                    if constexpr (requires(decltype(call) c){ c->WritesDone(); }){
                        if (write_completion_count == requested_count) {
                            call->WritesDone();
                        }
                    }
                } else if (puller.tag() == Operation::ReadCall){
                    read_finished = !puller.ok();
                    ++read_completion_count;
                    ASSERT_EQ(read_count, read_completion_count);
                    if (!read_finished) {
                        ASSERT_EQ(read_msg.text(), read_text);
                    }
                } else {
                    ASSERT_EQ(puller.tag(), Operation::WriteDone);
                    ASSERT_TRUE(puller.ok());
                }
            }

            if (write_count == write_completion_count && write_count < requested_count){
                call->Write(write_msg);
                ++write_count;
            }
            if (read_count == read_completion_count && !read_finished){
                call->Read(&read_msg);
                ++read_count;
            }
        }
        ASSERT_EQ(read_count, requested_count+1);
        ASSERT_EQ(write_count, requested_count);
    };
    auto client_thread = std::async([&] {
        CompletionQueuePuller puller(client_->CompletionQueue(), 500ms);
        auto call = StartClientCall();
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::OutgoingCall, true,
                            grpc::CompletionQueue::GOT_EVENT);

        core(call.get(), puller, "CLIENT", "SERVER");
        call->Finish();
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::FinishCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        EXPECT_TRUE(call->finish_status_.ok());
    });
    auto server_thread = std::async([&] {
        CompletionQueuePuller puller(server_->CompletionQueue(), 500ms);
        auto call = StartServerStream();
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::IncomingCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
        core(call.get(), puller, "SERVER", "CLIENT");

        call->Finish(grpc::Status());
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::FinishCall, true,
                            grpc::CompletionQueue::GOT_EVENT);
    });
    client_thread.wait();
    server_thread.wait();
}

TEST_F(BidiStreamFixture, ServerBreaksStream) {
    using namespace std::chrono_literals;
    StartServer();
    ConnectClientStubToServer();

    CompletionQueuePuller client_puller(client_->CompletionQueue(), 500ms);
    auto client_call = StartClientCall();
    CompletionQueuePuller server_puller(server_->CompletionQueue(), 500ms);
    auto server_call = StartServerStream();
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);

    server_call->Finish(grpc::Status(grpc::StatusCode::INTERNAL, "Just break"));
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::FinishCall, true,
                        grpc::CompletionQueue::GOT_EVENT);

    StringMsg read_msg;
    client_call->Read(&read_msg);
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::ReadCall, false,
                        grpc::CompletionQueue::GOT_EVENT);
    client_call->Finish();
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true,
                        grpc::CompletionQueue::GOT_EVENT);
    EXPECT_FALSE(client_call->finish_status_.ok());
}

TEST_F(BidiStreamFixture, ClientBreaksStream) {
    using namespace std::chrono_literals;
    StartServer();
    ConnectClientStubToServer();

    CompletionQueuePuller client_puller(client_->CompletionQueue(), 500ms);
    auto client_call = StartClientCall();
    CompletionQueuePuller server_puller(server_->CompletionQueue(), 500ms);
    auto server_call = StartServerStream();
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::OutgoingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::IncomingCall, true,
                        grpc::CompletionQueue::GOT_EVENT);

    client_call.reset();

    StringMsg read_msg;
    server_call->Read(&read_msg);
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::ReadCall, false,
                        grpc::CompletionQueue::GOT_EVENT);

    StringMsg write_msg;
    server_call->Write(write_msg);
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::WriteCall, false,
                        grpc::CompletionQueue::GOT_EVENT);

    server_call->Finish(grpc::Status(grpc::StatusCode::INTERNAL, "Just break"));
    ASSERT_EQ(server_puller.Pull(), grpc::CompletionQueue::GOT_EVENT);
}
