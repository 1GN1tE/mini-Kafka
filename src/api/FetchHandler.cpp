#include "api/FetchHandler.hpp"
#include "storage/IMetadataStore.hpp"
#include "protocol/BufferReader.hpp"

FetchHandler::FetchHandler(std::shared_ptr<IMetadataStore> store)
    : metadata_store(store) {}

kafka::protocol::Response FetchHandler::handle(const kafka::protocol::Request &request)
{
    kafka::protocol::BufferReader reader(request.body);

    reader.readInt32(); // max_wait_ms
    reader.readInt32(); // min_bytes
    reader.readInt32(); // max_bytes
    reader.readInt8();  // isolation_level
    reader.readInt32(); // session_id
    reader.readInt32(); // session_epoch

    int32_t topic_array_size = reader.readInt8() - 1;
    std::vector<uint8_t> topic_id;

    if (topic_array_size > 0)
    {
        // A UUID is 16 bytes. Read the topic_id from the first topic entry.
        topic_id = reader.readBytes(16);
    }

    // Construct the response
    kafka::protocol::Response response(request.correlation_id);

    response.writeInt8(0);  // top-level tagged fields
    response.writeInt32(0); // throttle_time_ms

    response.writeInt16(0); // error_code
    response.writeInt32(0); // session_id

    response.writeInt8(topic_array_size + 1); // num_responses

    if (topic_array_size == 0)
    {
        response.writeInt8(0); // No topics
        return response;
    }

    // Start Topic Response
    {
        response.writeBytes(topic_id); // topic_id

        response.writeInt8(1 + 1); // 'partitions' array

        // Start Partition Response
        {
            response.writeInt32(0); // partition_index

            bool is_known = metadata_store->is_uuid_known(topic_id);

            if (is_known)
            {
                response.writeInt16(0); // error_code
            }
            else
            {
                response.writeInt16(100); // error_code
            }

            response.writeInt64(-1); // high_watermark
            response.writeInt64(-1); // last_stable_offset
            response.writeInt64(-1); // log_start_offset

            response.writeInt8(0 + 1); // aborted_transactions
            response.writeInt8(0);     // tagged fields

            response.writeInt32(-1); // preferred_read_replica

            response.writeInt8(1); // record_set
            response.writeInt8(0); // tagged fields
        }
        // End Partition Response

        response.writeInt8(0); // tagged fields
    }
    // End Topic Response

    // The provided examples show a final byte for tagged fields.
    response.writeInt8(0); // Final tag buffer

    return response;
}