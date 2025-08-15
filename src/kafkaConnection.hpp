#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <vector>
#include <stdexcept>

#include "request.hpp"
#include "response.hpp"

template <typename T>
T read_big_endian(const char *data)
{
    T value;
    std::memcpy(&value, data, sizeof(T));
    if constexpr (sizeof(T) == 2)
    {
        return ntohs(value);
    }
    else if constexpr (sizeof(T) == 4)
    {
        return ntohl(value);
    }
    return value;
}

class kafkaConnection
{
public:
    kafkaConnection(int fd) : m_socket_fd(fd) {}
    
    void processRequest()
    {
        while (true)
        {
            Request req;
            if (!read_request(req))
            {
                std::cerr << "Client closed connection or invalid request.\n";
                break;
            }

            Response response(req.correlation_id);
            auto version = req.request_api_version;

            if (version >= 0 && version <= 4)
            {
                response.append(htons(0));                // error code
                response.append(static_cast<uint8_t>(2)); // Tag field for api key
                response.append(htons(18));               // API index (18 == ApiVersions)
                response.append(htons(0));                // Min versions
                response.append(htons(4));                // Max version
                response.append(static_cast<uint8_t>(0)); // Tag field for api key
                response.append(htonl(0));                // Throttle field
                response.append(static_cast<uint8_t>(0)); // Tag field for response
            }
            else
            {
                response.append(htons(35)); // error code
            }

            sendResponse(response);
        }
    }

private:
    int m_socket_fd;

    bool recv_all(char *buf, size_t len)
    {
        size_t total = 0;
        while (total < len)
        {
            ssize_t n = recv(m_socket_fd, buf + total, len - total, 0);
            if (n == 0)
                return false; // connection closed
            if (n < 0)
            {
                if (errno == EINTR)
                    continue;
                return false;
            }
            total += n;
        }
        return true;
    }

    bool read_request(Request &req)
    {
        char buffer[1024];

        // Read message size
        if (!recv_all(buffer, 4))
            return false;

        req.message_size = read_big_endian<int32_t>(buffer);
        if (req.message_size > sizeof(buffer))
        {
            std::cerr << "Message too large for buffer: " << req.message_size << "\n";
            return false;
        }

        // Read the rest of the message
        if (!recv_all(buffer, req.message_size))
            return false;

        const char *ptr = buffer;
        req.request_api_key = read_big_endian<int16_t>(ptr);
        ptr += 2;
        req.request_api_version = read_big_endian<int16_t>(ptr);
        ptr += 2;
        req.correlation_id = read_big_endian<int32_t>(ptr);
        ptr += 4;

        uint16_t client_id_len = read_big_endian<uint16_t>(ptr);
        ptr += 2;
        if (client_id_len > 0 && static_cast<size_t>(ptr - buffer + client_id_len) <= sizeof(buffer))
        {
            req.client_id.assign(ptr, ptr + client_id_len);
        }
        else
        {
            req.client_id.clear();
        }

        return true;
    }

    void sendResponse(Response &response)
    {
        const auto &payload = response.getData();

        // Calculate total size: message_size + correlation_id + payload
        size_t total_size = sizeof(int32_t) + sizeof(int32_t) + payload.size();
        std::vector<char> buffer(total_size);

        // Fill the buffer
        int32_t message_size = htonl(response.getMessageSize());
        int32_t correlation_id = htonl(response.getCorrelationId());

        size_t offset = 0;
        std::memcpy(buffer.data() + offset, &message_size, sizeof(message_size));
        offset += sizeof(message_size);

        std::memcpy(buffer.data() + offset, &correlation_id, sizeof(correlation_id));
        offset += sizeof(correlation_id);

        std::memcpy(buffer.data() + offset, payload.data(), payload.size());

        // Send all at once
        ssize_t sent = send(m_socket_fd, buffer.data(), buffer.size(), 0);
        if (sent < 0)
        {
            perror("send failed");
        }
        else
        {
            std::cout << "Sent " << sent << " bytes in one send()\n";
        }
    }
};
