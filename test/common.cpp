#include "common.h"

void DrainCompletionQueue(grpc::CompletionQueue &cq) {
    void *tag;
    bool ok;
    while (cq.Next(&tag, &ok))
        ;
}

testing::AssertionResult
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

bool IsStatusEquals(const grpc::Status &stat1, const grpc::Status &stat2) {
    return stat1.error_code() == stat2.error_code() && stat1.error_message() == stat2.error_message();
}

void ReadMessagesUntilOk(grpc::internal::AsyncReaderInterface<mypkg::StringMsg>& reader, mypkg::StringMsg *readMessage, CompletionQueuePuller &puller, int &readMessageCount,
                         int maxReadCount) {
    for (readMessageCount = 0; readMessageCount < maxReadCount;) {
        readMessage->set_text("not initialized");
        reader.Read(readMessage, reinterpret_cast<void *>(Operation::ReadCall));
        if (puller.Pull() != grpc::CompletionQueue::GOT_EVENT || !puller.ok())
            break;
        ++readMessageCount;
        ASSERT_EQ(puller.tag(), Operation::ReadCall);
        ASSERT_EQ(std::to_string(readMessageCount), readMessage->text());
    }
}

void SendMessagesUntilOk(grpc::internal::AsyncWriterInterface<mypkg::StringMsg>& writer, CompletionQueuePuller &puller, int max_count, int &sentMessageCount,
                         std::optional<std::chrono::milliseconds> delay) {
    while (sentMessageCount < max_count) {
        ++sentMessageCount;
        mypkg::StringMsg sentMessage;
        sentMessage.set_text(std::to_string(sentMessageCount));
        auto opt = grpc::WriteOptions();
        if (sentMessageCount + 1 < max_count)
            opt.set_buffer_hint();
        writer.Write(sentMessage, reinterpret_cast<void *>(Operation::WriteCall));

        if (puller.Pull() != grpc::CompletionQueue::GOT_EVENT || !puller.ok() || puller.tag() == Operation::AsyncDone)
            break;
        ASSERT_EQ(puller.tag(), Operation::WriteCall);
        if (delay) {
            std::this_thread::sleep_for(*delay);
        }
    }
}
