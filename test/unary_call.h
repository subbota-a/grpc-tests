#pragma once
#include "common.h"
#include <chrono>

using mypkg::StringMsg;

class UnaryClientCall final {
    grpc::ClientContext context_;
    std::unique_ptr<grpc::ClientAsyncResponseReader<StringMsg>> reader_;
    CompletionQueueTag tag_;

public:
    grpc::Status finish_status_;
    StringMsg response_;

    UnaryClientCall(Client &client, const StringMsg &request,
                    std::optional<std::chrono::system_clock::time_point> deadline)
        : tag_{this, Operation::OutgoingCall} {
        if (deadline)
            context_.set_deadline(*deadline);
        reader_ = client.AsyncUnary(&context_, request);
        reader_->Finish(&response_, &finish_status_, &tag_);
    }
    void TryCancel() { context_.TryCancel(); }
};

class UnaryServerCall final {
    grpc::ServerContext context_;
    std::unique_ptr<grpc::ServerAsyncResponseWriter<StringMsg>> writer_;
    CompletionQueueTag tag_;
    CompletionQueueTag async_done_tag_;

public:
    StringMsg request_;

    explicit UnaryServerCall(Server &server, bool async_done)
        : writer_(std::make_unique<grpc::ServerAsyncResponseWriter<StringMsg>>(&context_)),
          tag_{this, Operation::IncomingCall}, async_done_tag_{this, Operation::AsyncDone} {
        if (async_done) {
            context_.AsyncNotifyWhenDone(&async_done_tag_);
        }
        server.RequestUnary(&context_, &request_, writer_.get(), &tag_);
    }
    void Finish(const StringMsg &response) {
        tag_.operation = Operation::FinishCall;
        writer_->Finish(response, grpc::Status(), &tag_);
    }
};

class UnaryFixture : public BaseFixture {
protected:
    [[nodiscard]] std::unique_ptr<UnaryServerCall> StartServerRequest(bool async_done = false) {
        return std::make_unique<UnaryServerCall>(*server_, async_done);
    }
    [[nodiscard]] std::unique_ptr<UnaryClientCall>
    StartClientCall(const StringMsg &request, std::optional<std::chrono::system_clock::time_point> deadline = {}) {
        return std::make_unique<UnaryClientCall>(*client_, request, deadline);
    }
};
