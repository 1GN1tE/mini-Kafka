#pragma once
#include "api/IApiHandler.hpp"

class FetchHandler : public IApiHandler
{
public:
    kafka::protocol::Response handle(const kafka::protocol::Request &request) override;
};