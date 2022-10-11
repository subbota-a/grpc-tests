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
    void TryCancel() { context_.TryCancel(); }
    void Write(const StringMsg &msg) { stream_.Write(msg, reinterpret_cast<void *>(Operation::WriteCall)); }
    void Read(StringMsg *msg) { stream_.Read(msg, reinterpret_cast<void *>(Operation::ReadCall)); }
    void Finish(grpc::Status finish_status) {
        stream_.Finish(finish_status, reinterpret_cast<void *>(Operation::FinishCall));
    }
};
}// namespace

class BidiStreamFixture : public BaseFixture {
protected:
    [[nodiscard]] std::unique_ptr<ServerCall> StartServerStream() { return std::make_unique<ServerCall>(*server_); }
    [[nodiscard]] std::unique_ptr<ClientCall>
    StartClientCall(std::optional<std::chrono::system_clock::time_point> deadline = {}) {
        return std::make_unique<ClientCall>(*client_, deadline);
    }
    struct ReadWriteResult {
        ReadWriteResult(int read, int write, bool got_finish) : read(read), write(write), got_finish(got_finish) {}
        int read;
        int write;
        bool got_finish;
    };

    template<typename CallType>
    ReadWriteResult ReadWrite(CallType* call, CompletionQueuePuller &puller, const char *write_text,
                              const char *read_text, std::optional<int> max_write_count) {
        StringMsg write_msg;
        StringMsg read_msg;
        write_msg.set_text(write_text);
        int write_count = 0;
        bool write_running = false;
        int read_count = 0;
        bool read_running = false;
        bool read_finished = false;
        bool write_finished = false;
        bool stop_writing = max_write_count && *max_write_count == 0;
        bool got_finish = false;
        while (!write_finished || !read_finished) {
            if (read_running || write_running) {
                puller.Pull();
                EXPECT_EQ(puller.status(), grpc::CompletionQueue::NextStatus::GOT_EVENT);
                if (puller.tag() == Operation::WriteCall) {
                    write_running = false;
                    EXPECT_TRUE(puller.ok());
                    ++write_count;
                    if (max_write_count && write_count == *max_write_count
                        || !max_write_count && read_finished) {
                        stop_writing = true;
                    }
                } else if (puller.tag() == Operation::ReadCall) {
                    read_finished = !puller.ok();
                    read_running = false;
                    if (puller.ok()) {
                        ++read_count;
                        EXPECT_EQ(read_msg.text(), read_text);
                    }
                } else if (puller.tag() == Operation::WriteDone){
                    EXPECT_TRUE(puller.ok());
                    write_running = false;
                    write_finished = true;
                } else {
                    EXPECT_EQ(puller.tag(), Operation::FinishCall);
                    got_finish = true;
                }
            }

            if (!write_running && !write_finished) {
                if (!stop_writing) {
                    call->Write(write_msg);
                    write_running = true;
                } else {
                    if constexpr (requires(decltype(call) c) { c->WritesDone(); }) {
                        write_running = true;
                        call->WritesDone();
                    } else {
                        write_finished = true;
                    }
                }
            }

            if (!write_running && !stop_writing) {
                call->Write(write_msg);
                write_running = true;
            }
            if (!read_running && !read_finished) {
                call->Read(&read_msg);
                read_running = true;
            }
        }
        return ReadWriteResult{read_count, write_count, got_finish};
    }
};

TEST_F(BidiStreamFixture, EachSideSendsCountOfMessagesAndReadsItInParallel) {
    using namespace std::chrono_literals;
    StartServer();
    ConnectClientStubToServer();
    const int requested_count = 1'000;
    auto client_thread = std::async([&] {
        CompletionQueuePuller puller(client_->CompletionQueue(), 500ms);
        auto call = StartClientCall();
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::OutgoingCall, true, grpc::CompletionQueue::GOT_EVENT);

        auto result = ReadWrite(call.get(), puller, "CLIENT", "SERVER", requested_count);
        EXPECT_EQ(result.read, requested_count);
        EXPECT_EQ(result.write, requested_count);
        EXPECT_FALSE(result.got_finish);

        call->Finish();
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
        EXPECT_TRUE(call->finish_status_.ok());
    });
    auto server_thread = std::async([&] {
        CompletionQueuePuller puller(server_->CompletionQueue(), 500ms);
        auto call = StartServerStream();
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::IncomingCall, true, grpc::CompletionQueue::GOT_EVENT);
        auto result = ReadWrite(call.get(), puller, "SERVER", "CLIENT", requested_count);
        EXPECT_EQ(result.read, requested_count);
        EXPECT_EQ(result.write, requested_count);

        call->Finish(grpc::Status());
        ASSERT_PRED_FORMAT4(AssertCompletion, puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
    });
    client_thread.wait();
    server_thread.wait();
}

TEST_F(BidiStreamFixture, ServerWritesUntilClientWritesDone) {
    using namespace std::chrono_literals;
    StartServer();
    ConnectClientStubToServer();
    int client_message_count = 1'000;
    auto client_thread = std::async([&] {
        CompletionQueuePuller puller(client_->CompletionQueue(), 500ms);
        auto call = StartClientCall();
        EXPECT_PRED_FORMAT4(AssertCompletion, puller, Operation::OutgoingCall, true, grpc::CompletionQueue::GOT_EVENT);
        auto result = ReadWrite(call.get(), puller, "CLIENT", "SERVER", client_message_count);
        EXPECT_EQ(result.write, client_message_count);
        EXPECT_GT(result.read, client_message_count/10);
        EXPECT_FALSE(result.got_finish);

        call->Finish();
        EXPECT_PRED_FORMAT4(AssertCompletion, puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
        EXPECT_EQ(call->finish_status_.error_code(), grpc::StatusCode::ABORTED);
        EXPECT_EQ(call->finish_status_.error_message(), "ABORTED");
    });
    auto server_thread = std::async([&] {
        CompletionQueuePuller puller(server_->CompletionQueue(), 500ms);
        auto call = StartServerStream();
        EXPECT_PRED_FORMAT4(AssertCompletion, puller, Operation::IncomingCall, true, grpc::CompletionQueue::GOT_EVENT);

        auto result = ReadWrite(call.get(), puller, "SERVER", "CLIENT", std::nullopt);
        EXPECT_EQ(result.read, client_message_count);
        EXPECT_GT(result.write, client_message_count/10);

        call->Finish(grpc::Status(grpc::StatusCode::ABORTED, "ABORTED"));
        EXPECT_PRED_FORMAT4(AssertCompletion, puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
    });
    client_thread.wait();
    server_thread.wait();
}

TEST_F(BidiStreamFixture, ClientFinishFirst) {
    using namespace std::chrono_literals;
    StartServer();
    ConnectClientStubToServer();
    int client_message_count = 1'000;
    auto client_thread = std::async([&] {
        CompletionQueuePuller puller(client_->CompletionQueue(), 500ms);
        auto call = StartClientCall();
        EXPECT_PRED_FORMAT4(AssertCompletion, puller, Operation::OutgoingCall, true, grpc::CompletionQueue::GOT_EVENT);

        call->Finish();

        auto result = ReadWrite(call.get(), puller, "CLIENT", "SERVER", client_message_count);
        EXPECT_EQ(result.write, client_message_count);
        EXPECT_GT(result.read, 0);

        if (!result.got_finish) {
            EXPECT_PRED_FORMAT4(AssertCompletion, puller, Operation::FinishCall, true,
                                grpc::CompletionQueue::GOT_EVENT);
        }
        EXPECT_EQ(call->finish_status_.error_code(), grpc::StatusCode::ABORTED);
        EXPECT_EQ(call->finish_status_.error_message(), "ABORTED");
    });
    auto server_thread = std::async([&] {
        CompletionQueuePuller puller(server_->CompletionQueue(), 500ms);
        auto call = StartServerStream();
        EXPECT_PRED_FORMAT4(AssertCompletion, puller, Operation::IncomingCall, true, grpc::CompletionQueue::GOT_EVENT);

        auto result = ReadWrite(call.get(), puller, "SERVER", "CLIENT", std::nullopt);
        EXPECT_EQ(result.read, client_message_count);
        EXPECT_GT(result.write, 0);

        call->Finish(grpc::Status(grpc::StatusCode::ABORTED, "ABORTED"));
        EXPECT_PRED_FORMAT4(AssertCompletion, puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
    });
    client_thread.wait();
    server_thread.wait();
}

TEST_F(BidiStreamFixture, ServerStopsAfterCallIsReceived) {
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
    if (grpc::Version() == "1.27.2") {
        server_call->TryCancel();
    }
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
    StringMsg read_msg;
    client_call->Read(&read_msg);

    server_call->Finish(grpc::Status(grpc::StatusCode::INTERNAL, "Just break"));
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);

    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::ReadCall, false, grpc::CompletionQueue::GOT_EVENT);
    client_call->Finish();
    ASSERT_PRED_FORMAT4(AssertCompletion, client_puller, Operation::FinishCall, true, grpc::CompletionQueue::GOT_EVENT);
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
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::ReadCall, false, grpc::CompletionQueue::GOT_EVENT);

    StringMsg write_msg;
    server_call->Write(write_msg);
    ASSERT_PRED_FORMAT4(AssertCompletion, server_puller, Operation::WriteCall, false, grpc::CompletionQueue::GOT_EVENT);

    server_call->Finish(grpc::Status(grpc::StatusCode::INTERNAL, "Just break"));
    ASSERT_EQ(server_puller.Pull(), grpc::CompletionQueue::GOT_EVENT);
}
