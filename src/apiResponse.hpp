#include "headers.hpp"
#include "response.hpp"
#include "request.hpp"
#include "broker.hpp"
#pragma once

struct api_key
{
    int16_t min_version;
    int16_t max_version;
    api_key() : min_version(0), max_version(0) {}
    api_key(int16_t min_v, int16_t max_v)
        : min_version(min_v), max_version(max_v) {}
};
class APIResponse
{
public:
    std::unordered_map<int16_t, api_key> api_versions;

    APIResponse()
    {
        // Initialize with some default API versions
        api_versions[18] = api_key(0, 4); // ApiVersions
        api_versions[75] = api_key(0, 0); // DescribeTopicPartitions
    }
    void handleResponseFor18(Response &response)
    {
        response.append(htons(0));                                      // error code
        response.append(static_cast<uint8_t>(api_versions.size() + 1)); // API version array+1 meaning length of array is 2
        // API 18 - ApiVersions
        response.append(htons(18)); // API key
        response.append(htons(0));  // min version
        response.append(htons(4));  // max version
        response.append(static_cast<uint8_t>(0));
        for (auto it = api_versions.begin(); it != api_versions.end(); ++it)
        {
            if (it->first == 18)
                continue; // Skip ApiVersions itself

            // Optional: another API
            response.append(htons(it->first));              // API key
            response.append(htons(it->second.min_version)); // min version
            response.append(htons(it->second.max_version)); // max version
            response.append(static_cast<uint8_t>(0));       // tag
        }

        response.append(htonl(0));                // throttle_time
        response.append(static_cast<uint8_t>(0)); // tag
    }
    void handleUnknownTopic(const std::string &topic, Response &res)
    {
        res.append(static_cast<uint8_t>(0));                // top-level tag buffer
        res.append(htonl(0));                               // throttle_time_ms
        res.append(static_cast<uint8_t>(2));                // array length +1
        res.append(htons(3));                               // error_code (UNKNOWN_TOPIC)
        res.append(static_cast<uint8_t>(topic.size() + 1)); // topic_name length+1
        
        for (char c : topic)
        {
            res.append(static_cast<uint8_t>(c));
            
        }
        for (int i = 0; i < 16; ++i)
            res.append(static_cast<uint8_t>(0)); // topic_id
        res.append(static_cast<uint8_t>(0));     // is_internal
        res.append(static_cast<uint8_t>(1));     // partition array length+1
        res.append(htonl(0x0df8));               // topic_authorized_operations
        res.append(static_cast<uint8_t>(0));     // partition tag buffer
        res.append(static_cast<uint8_t>(0xFF));  // next_cursor null
        res.append(static_cast<uint8_t>(0));     // final tag buffer
        
    }

    void handleResponsFor75(Request &req, Response &res, Broker &broker)
    {
        for (int i = 0; i < req.topics.size(); i++)
        {
            auto it = broker.topics.find(req.topics[i]);
            if (it != broker.topics.end())
            {
                // TODO: Handle the topic found case
            }
            else
            {
                handleUnknownTopic(req.topics[i], res);
            }
        }
    }
    void handleResponseFor35(Response &response)
    {
        response.append(htons(35));                // error code
        response.append(static_cast<uint8_t>(1)); // API version array+1 meaning length of array is 2
        // API 35 - DescribeTopicPartitions
        response.append(htons(35)); // API key
        response.append(htons(0));  // min version
        response.append(htons(0));  // max version
        response.append(static_cast<uint8_t>(0));

        response.append(htonl(0));                // throttle_time
        response.append(static_cast<uint8_t>(0)); // tag
    }
    Response createResponse(Request &req, Broker &broker)
    {
        Response response(req.correlation_id);
        auto version = req.request_api_version;

        if (version >= 0 && version <= 4)
        {
            switch (req.request_api_key)
            {
            case 18:
                handleResponseFor18(response);
                /* code */
                break;
                // Add more cases for other API keys as needed
            case 75:
                handleResponsFor75(req, response, broker);
                break;
            default:
                break;
            }
        }
        else
        {
            handleResponseFor35(response); // Default to 35 if version is out of range
        }
        return response;
    }
};