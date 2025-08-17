#include "headers.hpp"
#pragma once

class Response
{
private:
    std::int32_t m_message_size;
    std::int32_t corelation_id;
    std::vector<std::uint8_t> m_data;

public:
    Response(int32_t corelation_id)
    {
        m_message_size = 4; // Initial size for the correlation ID
        this->corelation_id = corelation_id;
    }
    void append(std::vector<std::uint8_t> contents)
    {
        m_data.insert(m_data.end(), contents.begin(), contents.end());
        m_message_size += contents.size();
    }
    void adjustMessageSize()
    {

        m_message_size = sizeof(corelation_id) + m_data.size();
    }
    template <typename T>
    void append(T value)
    {
        // Just treat the value as raw bytes and append
        auto bytes = reinterpret_cast<const uint8_t *>(&value);
        m_data.insert(m_data.end(), bytes, bytes + sizeof(value));
        m_message_size += sizeof(value);
    }
    std::int32_t getMessageSize()
    {
        return m_message_size;
    }
    std::int32_t getCorrelationId()
    {
        return corelation_id;
    }
    const std::vector<std::uint8_t> &getData()
    {
        return m_data;
    }
};