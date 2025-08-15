#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>

#include "request.hpp"
#include "response.hpp"

class kafkaConnection {
    public:
    void sendResponse(int socket_fd, Response& response) {
        // Send the message size first
        int32_t message_size = htonl(response.getMessageSize());
        send(socket_fd, &message_size, sizeof(message_size), 0);

        // Send the correlation ID
        int32_t correlation_id = htonl(response.getCorrelationId());
        send(socket_fd, &correlation_id, sizeof(correlation_id), 0);

        // Send the actual data
        const auto& data = response.getData();
        send(socket_fd, data.data(), data.size(), 0);
    }
    void processRequest(int socket_fd) {
        while(true){
            Request request(socket_fd);
            if (!request.flag) {
                std::cerr << "Failed to parse request" << std::endl;
                return;
            }
            Response response(request.getCollationId());
            auto version = request.getRequestApiVersion();
            if (version >= 0 && version <= 4) {
                response.append(htons(0)); // error code
                response.append(static_cast<uint8_t>(2)); // Tag field for api key
                response.append(htons(18)); // API index (18 == ApiVersions)
                response.append(htons(0)); // Min versions
                response.append(htons(4)); // Max version (at least 4)
                response.append(static_cast<uint8_t>(0)); // Tag field for api key
                response.append(htonl(0)); // Throttle field
                response.append(static_cast<uint8_t>(0)); // Tag field for response
            } else {
                response.append(htons(35)); // error code
            }
            sendResponse(socket_fd, response);
        }
    }
};