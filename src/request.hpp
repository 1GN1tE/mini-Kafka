#include <bits/stdc++.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
class Request
{
private:
    struct client_request
    {
        int32_t message_size;
        int16_t request_api_key;
        int16_t request_api_version;
        int32_t correlation_id;
        std::string client_id;
    };
    client_request request;

public:
    bool flag=false;
    Request(int socket_fd)
    {
        flag=parse(socket_fd);
        
    }
    bool parse(int socket_fd){
        // read fixed-size header first
        uint32_t msg_size_net;
        if (recv(socket_fd, &msg_size_net, sizeof(msg_size_net), 0) <= 0)
            return false;
        request.message_size = ntohl(msg_size_net);

        uint16_t api_key_net;
        recv(socket_fd, &api_key_net, sizeof(api_key_net), 0);
        request.request_api_key = ntohs(api_key_net);

        uint16_t api_ver_net;
        recv(socket_fd, &api_ver_net, sizeof(api_ver_net), 0);
        request.request_api_version = ntohs(api_ver_net);

        uint32_t corr_id_net;
        recv(socket_fd, &corr_id_net, sizeof(corr_id_net), 0);
        request.correlation_id = ntohl(corr_id_net);

        // read client_id length + data (Kafka style)
        uint16_t client_len_net;
        recv(socket_fd, &client_len_net, sizeof(client_len_net), 0);
        uint16_t client_len = ntohs(client_len_net);

        std::vector<char> buf(request.message_size);
        std::cout<<request.message_size<<std::endl;
        recv(socket_fd, buf.data(), buf.size(), 0);
        request.client_id.assign(buf.begin(), buf.end());
        return true;
    }
    std::int32_t getCollationId()
    {
        return request.correlation_id;
    }
    std::int16_t getRequestApiKey()
    {
        return request.request_api_key;
    }
    std::int16_t getRequestApiVersion()
    {
        return request.request_api_version;
    }
    std::int32_t getMessageSize()
    {
        return request.message_size;
    }
};