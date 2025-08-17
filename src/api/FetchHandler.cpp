#include "api/FetchHandler.hpp"

kafka::protocol::Response FetchHandler::handle(const kafka::protocol::Request &request)
{
    kafka::protocol::Response response(request.correlation_id);

    // This is a non-functional stub response for Fetch API v15
    response.writeInt8(0);  // tagged fields
    response.writeInt16(0); // error code
    response.writeInt32(0); // throttle_time_ms
    response.writeInt32(0); // session_id

    response.writeInt8(0); // topics array size (empty)
    response.writeInt8(0);  // final tag buffer

    return response;
}