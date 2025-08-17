#include "headers.hpp"
#include "response.hpp"
#include "request.hpp"
#include "parseMetadata.hpp"

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
        api_versions[0] = api_key(0, 11); // Produce
        api_versions[1] = api_key(0, 16); // Fetch
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
        // This function now only appends the details for a single unknown topic.
        res.append(htons(3));                               // error_code (UNKNOWN_TOPIC)
        res.append(static_cast<uint8_t>(topic.size() + 1)); // topic_name length+1

        for (char c : topic)
        {
            res.append(static_cast<uint8_t>(c));
        }
        for (int i = 0; i < 16; ++i)
        {
            res.append(static_cast<uint8_t>(0)); // topic_id
        }
        res.append(static_cast<uint8_t>(0));    // is_internal
        res.append(static_cast<uint8_t>(1));    // partition array length+1
        res.append(htonl(0x0df8));              // topic_authorized_operations
        res.append(static_cast<uint8_t>(0));    // partition tag buffer
        res.append(static_cast<uint8_t>(0xFF)); // next_cursor null
        res.append(static_cast<uint8_t>(0));    // final tag buffer
    }

    void addPartionInformation(Response &res)
    {
        for (int i = 0; i < 2; i++)
        {
            res.append(htons(0)); // partition error_code
            res.append(htonl(0)); // partition index
            res.append(htonl(1)); // leader_id
            res.append(htonl(0)); // leader_epoch

            res.append(static_cast<uint8_t>(2)); // replica nodes length+1 (array of 1 element)
            res.append(htonl(1));                // replica node 1

            res.append(static_cast<uint8_t>(2)); // ISR nodes length+1 (array of 1 element)
            res.append(htonl(1));                // ISR node 1

            res.append(static_cast<uint8_t>(1)); // eligible leader replicas length+1 (array of 0 elements)
            res.append(static_cast<uint8_t>(1)); // last known ELR length+1 (array of 0 elements)
            res.append(static_cast<uint8_t>(1)); // offline replicas length+1 (array of 0 elements)

            res.append(static_cast<uint8_t>(0)); // empty partition tag buffer
        }
    }

    void handleResponsFor75(const Request &req, Response &res)
    {
        uint8_t tagged_fields = 0;
        res.append(tagged_fields); // top-level tagged fields
        res.append(htonl(0));      // throttle_time_ms

        std::size_t Array_Length = req.topics.size() + 1;
        res.append(static_cast<uint8_t>(Array_Length)); // array length

        parseMetadata Image;
        int16_t error_code;

        // Iterate over all requested topics
        for (const auto &topic : req.topics)
        {
            bool known_name = Image.judgeTopicName(topic);

            // Append error code
            error_code = known_name ? htons(0) : htons(3);
            res.append(error_code);

            // Append topic name length + 1
            res.append(static_cast<uint8_t>(topic.size() + 1));

            // Append topic name
            for (char c : topic)
            {
                res.append(static_cast<uint8_t>(c));
            }

            // Append topic UUID (16 bytes) or zeros if unknown
            std::vector<uint8_t> topic_id = known_name ? Image.getTopicUuid(topic)
                                                       : std::vector<uint8_t>(16, 0);
            res.append(topic_id);

            // Append is_internal
            uint8_t Is_Internal = 0;
            res.append(Is_Internal);

            // Append partition array length + 1
            if (known_name)
            {
                std::vector<std::vector<uint8_t>> partitions = Image.getSerPartitions(topic_id);
                uint8_t partitions_length = partitions.size() + 1;
                res.append(partitions_length);

                for (auto &part : partitions)
                {
                    res.append(part);
                }
            }
            else
            {
                uint8_t partitions_length = 1;
                res.append(partitions_length);
            }

            // Append authorized_operations
            res.append(htonl(0x00000df8));

            // Append topic tagged fields
            res.append(tagged_fields);
        }

        // Append next_cursor and final tagged fields
        res.append(static_cast<uint8_t>(0xFF));
        res.append(tagged_fields);
    }

    void handleResponseFor1(Response &response)
    {
        response.append(static_cast<uint8_t>(0)); // tagged fields
        response.append(htons(0));                // error code
        response.append(htonl(0));                // throttle_time_ms
        response.append(htonl(0));                // session_id
        response.append(static_cast<uint8_t>(0)); // tag
        response.append(static_cast<uint8_t>(0)); // final tag buffer
    }

    void handleResponseFor35(Response &response)
    {
        response.append(htons(35));               // error code
        response.append(static_cast<uint8_t>(1)); // API version array+1 meaning length of array is 2
        // API 35 - DescribeTopicPartitions
        response.append(htons(35)); // API key
        response.append(htons(0));  // min version
        response.append(htons(0));  // max version
        response.append(static_cast<uint8_t>(0));

        response.append(htonl(0));                // throttle_time
        response.append(static_cast<uint8_t>(0)); // tag
    }

    Response createResponse(Request &req)
    {
        Response response(req.correlation_id);
        auto version = req.request_api_version;
        std::cout << "Request API Key: " << req.request_api_key << ", Version: " << version << std::endl;
        auto it = api_versions.find(req.request_api_key);
        if (it != api_versions.end() && (version >= it->second.min_version && version <= it->second.max_version))
        {
            std::cout << "Found" << std::endl;
            switch (req.request_api_key)
            {
            case 1:
                handleResponseFor1(response);
                break;
            case 18:
                handleResponseFor18(response);
                break;
            case 75:
                handleResponsFor75(req, response);
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