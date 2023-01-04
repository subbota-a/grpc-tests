#pragma once
#include "common.h"
#include <chrono>

using mypkg::StringMsg;

class UnaryClientCall final {
    grpc::ClientContext context_;
    std::unique_ptr<grpc::ClientAsyncResponseReader<StringMsg>> reader_;

public:
    grpc::Status finish_status_;
    StringMsg response_;

    UnaryClientCall(Client &client, const StringMsg &request,
                    std::optional<std::chrono::system_clock::time_point> deadline) {
        if (deadline)
            context_.set_deadline(*deadline);
        reader_ = client.AsyncUnary(&context_, request);
        reader_->Finish(&response_, &finish_status_, reinterpret_cast<void *>(Operation::OutgoingCall));
    }
    void TryCancel() { context_.TryCancel(); }
};

class UnaryServerCall final {
    grpc::ServerContext context_;
    std::unique_ptr<grpc::ServerAsyncResponseWriter<StringMsg>> writer_;

public:
    StringMsg request_;

    explicit UnaryServerCall(Server &server, bool async_done)
        : writer_(std::make_unique<grpc::ServerAsyncResponseWriter<StringMsg>>(&context_)) {
        if (async_done) {
            context_.AsyncNotifyWhenDone(reinterpret_cast<void *>(Operation::AsyncDone));
        }
        server.RequestUnary(&context_, &request_, writer_.get(),
                            reinterpret_cast<void *>(Operation::IncomingCall));
    }
    void Finish(const StringMsg& response) {
        writer_->Finish(response, grpc::Status(), reinterpret_cast<void *>(Operation::FinishCall));
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
