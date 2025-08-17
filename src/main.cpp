#include "headers.hpp"
#include "kafkaConnection.hpp"

#define WORKER_THREADS 4
std::queue<int> clientReqest;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

void *handleClientRequests(void *arg)
{
    while (true)
    {
        pthread_mutex_lock(&lock);
        if (!clientReqest.empty())
        {
            int client_fd = clientReqest.front();
            clientReqest.pop();
            pthread_mutex_unlock(&lock);
            kafkaConnection connection(client_fd);
            connection.processRequest();
            close(client_fd);
        }
        else
        {
            pthread_mutex_unlock(&lock);
            usleep(1000); // Sleep for a short time to avoid busy waiting
        }
    }
}

int main(int argc, char *argv[])
{
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;
    pthread_t threads[WORKER_THREADS];

    // Create Worker Threads
    for (int i = 0; i < WORKER_THREADS; i++)
    {
        if (pthread_create(&threads[i], nullptr, handleClientRequests, nullptr) != 0)
        {
            std::cout << "Error in creating threads" << std::endl;
            return -1;
        }
    }
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0)
    {
        std::cerr << "Failed to create server socket: " << std::endl;
        return 1;
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
    {
        close(server_fd);
        std::cerr << "setsockopt failed: " << std::endl;
        return 1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<struct sockaddr *>(&server_addr), sizeof(server_addr)) != 0)
    {
        close(server_fd);
        std::cerr << "Failed to bind to port 9092" << std::endl;
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0)
    {
        close(server_fd);
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";

    while (true)
    {
        struct sockaddr_in client_addr{};
        socklen_t client_addr_len = sizeof(client_addr);

        std::cerr << "Logs from your program will appear here!\n";

        int client_fd = accept(server_fd, reinterpret_cast<struct sockaddr *>(&client_addr), &client_addr_len);
        std::cout << "Client connected\n";

        pthread_mutex_lock(&lock);
        clientReqest.push(client_fd);
        pthread_mutex_unlock(&lock);
    }
    
    close(server_fd);
    return 0;
}