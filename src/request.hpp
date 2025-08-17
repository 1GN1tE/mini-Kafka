#include "headers.hpp"
#pragma once

class Request
{

public:
    int32_t message_size;
    int16_t request_api_key;
    int16_t request_api_version;
    int32_t correlation_id;
    std::string client_id;
    std::vector<std::string> topics;
    int32_t partion_count;
    std::int32_t getCollationId()
    {
        return correlation_id;
    }
    std::int16_t getRequestApiKey()
    {
        return request_api_key;
    }
    std::int16_t getRequestApiVersion()
    {
        return request_api_version;
    }
    std::int32_t getMessageSize()
    {
        return message_size;
    }
};