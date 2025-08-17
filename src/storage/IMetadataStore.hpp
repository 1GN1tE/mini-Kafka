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

    virtual bool is_topic_known(const std::string &topic_name) const = 0;

    virtual bool is_uuid_known(const std::vector<uint8_t> &uuid) const = 0;

    virtual std::vector<uint8_t> get_topic_uuid(const std::string &topic_name) const = 0;

    virtual std::vector<std::vector<uint8_t>> get_serialized_partitions(const std::vector<uint8_t> &topic_id) const = 0;
};