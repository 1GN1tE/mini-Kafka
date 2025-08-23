#include "api/FetchHandler.hpp"
#include "storage/IMetadataStore.hpp"
#include "protocol/BufferReader.hpp"

#include <cstdint>
#include <vector>
#include <array>
#include <memory>

// Represents the data for a single partition to be fetched.
struct PartitionFetchInfo
{
    int32_t index;
    int32_t current_leader_epoch;
    int64_t fetch_offset;
    int32_t last_fetched_epoch;
    int64_t log_start_offset;
    int32_t partition_max_bytes;
};

// Represents a topic to be fetched, containing its ID and a list of partitions.
struct TopicFetchInfo
{
    std::array<uint8_t, 16> id;
    std::vector<PartitionFetchInfo> partitions;
};

class FetchRequestData
{
public:
    int32_t max_wait_ms;
    int32_t min_bytes;
    int32_t max_bytes;
    int8_t isolation_level;
    int32_t session_id;
    int32_t session_epoch;
    std::vector<TopicFetchInfo> topics;

    // A static factory method to encapsulate the entire parsing logic.
    static FetchRequestData parse(kafka::protocol::BufferReader &reader)
    {
        FetchRequestData request_data;

        // Read header fields
        request_data.max_wait_ms = reader.readInt32();
        request_data.min_bytes = reader.readInt32();
        request_data.max_bytes = reader.readInt32();
        request_data.isolation_level = reader.readInt8();
        request_data.session_id = reader.readInt32();
        request_data.session_epoch = reader.readInt32();

        // The Kafka protocol often uses N-1 for array sizes.
        int32_t topic_array_size = reader.readInt8() - 1;

        if (topic_array_size > 0)
        {
            request_data.topics.reserve(topic_array_size); // Pre-allocate for efficiency
        }

        for (int i = 0; i < topic_array_size; ++i)
        {
            TopicFetchInfo current_topic;

            std::vector<uint8_t> topic_id_vec = reader.readBytes(16);
            if (topic_id_vec.size() == 16)
            {
                std::copy(topic_id_vec.begin(), topic_id_vec.end(), current_topic.id.begin());
            }

            int32_t partition_array_size = reader.readInt8() - 1;
            if (partition_array_size > 0)
            {
                current_topic.partitions.reserve(partition_array_size);
            }

            for (int j = 0; j < partition_array_size; ++j)
            {
                // Create a partition in-place using the data from the reader
                current_topic.partitions.emplace_back(PartitionFetchInfo{
                    .index = reader.readInt32(),
                    .current_leader_epoch = reader.readInt32(),
                    .fetch_offset = reader.readInt64(),
                    .last_fetched_epoch = reader.readInt32(),
                    .log_start_offset = reader.readInt64(),
                    .partition_max_bytes = reader.readInt32()});
            }
            request_data.topics.push_back(std::move(current_topic));
        }

        return request_data;
    }
};

FetchHandler::FetchHandler(std::shared_ptr<IMetadataStore> store)
    : metadata_store(store) {}

kafka::protocol::Response FetchHandler::handle(const kafka::protocol::Request &request)
{
    kafka::protocol::BufferReader reader(request.body);
    kafka::protocol::Response response(request.correlation_id);

    // Parse the request
    FetchRequestData request_data = FetchRequestData::parse(reader);

    response.writeInt8(0);  // top-level tagged fields
    response.writeInt32(0); // throttle_time_ms

    response.writeInt16(0); // error_code
    response.writeInt32(0); // session_id

    // Check if there are any topics to process
    if (request_data.topics.empty())
    {
        response.writeInt8(0 + 1); // num_responses = 0, encoded as 1
        response.writeInt8(0);     // Final tag buffer
        return response;
    }

    response.writeInt8(request_data.topics.size() + 1); // num_responses

    // Start Topic Response
    for (const auto &topic : request_data.topics)
    {
        std::vector<uint8_t> topic_id_vec(topic.id.begin(), topic.id.end());
        response.writeBytes(topic_id_vec);

        response.writeInt8(topic.partitions.size() + 1); // 'partitions' array size

        for (const auto &partition : topic.partitions)
        {
            response.writeInt32(partition.index); // partition index

            bool is_known = metadata_store->is_uuid_known(topic_id_vec);

            if (is_known)
            {
                response.writeInt16(0); // error_code: NO_ERROR
            }
            else
            {
                response.writeInt16(100); // error_code: UNKNOWN_TOPIC_ID
            }

            response.writeInt64(-1); // high_watermark
            response.writeInt64(-1); // last_stable_offset
            response.writeInt64(-1); // log_start_offset

            response.writeInt8(0 + 1); // aborted_transactions
            response.writeInt8(0);     // tagged fields for partition data

            response.writeInt32(-1); // preferred_read_replica

            if (is_known)
            {
                std::vector<uint8_t> recBatch = metadata_store->getEntireRecBatch(topic_id_vec, partition.index);
                response.writeInt8(2); // TODO
                response.writeBytes(recBatch);
            }
            else
            {
                response.writeInt8(0 + 1); // Empty record_set (size 0)
            }

            response.writeInt8(0); // tagged fields for partition response
        }

        response.writeInt8(0); // tagged fields for topic response
    }

    response.writeInt8(0); // Final tag buffer

    return response;
}