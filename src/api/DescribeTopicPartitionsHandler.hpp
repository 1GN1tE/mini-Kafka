#pragma once
#include "api/IApiHandler.hpp"
#include <memory>

// Forward-declare the interface to avoid including the full header here
class IMetadataStore;

class DescribeTopicPartitionsHandler : public IApiHandler
{
public:
    // We use dependency injection to provide the data store.
    explicit DescribeTopicPartitionsHandler(std::shared_ptr<IMetadataStore> metadata_store);

    kafka::protocol::Response handle(const kafka::protocol::Request &request) override;

private:
    std::shared_ptr<IMetadataStore> metadata_store;
};