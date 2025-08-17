#pragma once

#include <string>
#include <vector>
#include <cstdint>

/**
 * @brief An interface for a data store that provides Kafka topic and partition metadata.
 *
 * This abstraction allows the server's API logic to be decoupled from the
 * specific source of the metadata (e.g., a KRaft log file, an in-memory map, etc.).
 */
class IMetadataStore
{
public:
    virtual ~IMetadataStore() = default;

    /**
     * @brief Checks if a topic with the given name exists in the store.
     * @param topic_name The name of the topic to check.
     * @return true if the topic is known, false otherwise.
     */
    virtual bool is_topic_known(const std::string &topic_name) const = 0;

    /**
     * @brief Gets the 16-byte UUID for a given topic name.
     * @param topic_name The name of the topic.
     * @return A 16-byte vector representing the topic's UUID, or a zeroed vector if not found.
     */
    virtual std::vector<uint8_t> get_topic_uuid(const std::string &topic_name) const = 0;

    /**
     * @brief Gets the pre-serialized binary partition data for a given topic UUID.
     *
     * The returned data for each partition is in the exact binary format expected
     * by the Kafka protocol for a DescribeTopicPartitions response.
     *
     * @param topic_id The 16-byte UUID of the topic.
     * @return A vector where each element is a byte vector representing one serialized partition.
     */
    virtual std::vector<std::vector<uint8_t>> get_serialized_partitions(const std::vector<uint8_t> &topic_id) const = 0;
};