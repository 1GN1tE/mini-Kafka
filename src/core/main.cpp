#include "core/Server.hpp"
#include "core/ThreadPool.hpp"
#include "api/ApiRouter.hpp"
#include "storage/KRaftMetadataStore.hpp"
#include "api/ApiVersionsHandler.hpp"
#include "api/DescribeTopicPartitionsHandler.hpp"
#include "api/FetchHandler.hpp"
#include <iostream>
#include <memory>

int main(int argc, char *argv[])
{
    // Unbuffer output for immediate logging
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    // if (argc != 2)
    // {
    //     std::cerr << "Usage: " << argv[0] << " <path_to_kraft_log_file>\n";
    //     return 1;
    // }

    const std::string metadata_log_path = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    const int port = 9092;
    const int num_threads = 4;

    try
    {
        // Setup the data source
        auto metadataStore = std::make_shared<KRaftMetadataStore>(metadata_log_path);
        std::cout << "Successfully parsed metadata log file.\n";

        // Setup the API routing logic
        auto apiRouter = std::make_shared<ApiRouter>();

        // Pass the router to the ApiVersionsHandler so it can query the API list.
        auto apiVersionsHandler = std::make_unique<ApiVersionsHandler>(apiRouter);

        // apiRouter->registerHandler(0, 0, 11, std::make_unique<ProduceHandler>());
        apiRouter->registerHandler(1, 0, 16, std::make_unique<FetchHandler>(metadataStore));
        apiRouter->registerHandler(18, 0, 4, std::move(apiVersionsHandler));
        apiRouter->registerHandler(75, 0, 0, std::make_unique<DescribeTopicPartitionsHandler>(metadataStore));

        std::cout << "API handlers registered.\n";

        // Setup the thread pool
        auto threadPool = std::make_shared<ThreadPool>(num_threads);
        std::cout << "Thread pool with " << num_threads << " workers created.\n";

        // Start the server
        Server server(port, threadPool, apiRouter);
        std::cout << "Server starting on port " << port << "...\n";
        server.start();
    }
    catch (const std::exception &e)
    {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}