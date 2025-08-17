#pragma once
#include <vector>
#include <cstdint>
#include <set>
#include <map>
#include <string>

class parseMetadata
{
private:
    std::vector<uint8_t> cluster_metadata = {};
    std::map<int64_t, int32_t> batch_info = {};
    std::vector<int32_t> records_num = {};
    std::set<std::string> topic_name = {};
    std::map<std::string, std::vector<uint8_t>> nameToTopicId = {};

    std::map<std::vector<uint8_t>, std::vector<std::vector<uint8_t>>> topicToPars = {};

    std::vector<std::vector<uint8_t>> partitions = {};
    std::size_t offsetToRec = 0;

    int64_t toBigEndian(int64_t littleEndianVal);
    int64_t toLittleEndian(int64_t bigEndianVal);
    std::pair<int32_t, std::size_t> readZigZagVarint(std::size_t offset);
    std::pair<uint32_t, std::size_t> readUnsignedVarint(std::size_t offset);

    void getBatch_info();
    void toRecords();
    void getRecords_num();
    std::size_t getRecToValue(std::size_t offset);
    std::size_t toNextBatch(std::size_t offset);

    void parseTopicHelper(std::size_t offset);
    void parseTopic();

    void topicMatchPars(std::vector<uint8_t> uuid, std::vector<uint8_t> par);

    std::vector<uint8_t> parseParsHelper(std::size_t offset);
    void parsePars();

public:
    parseMetadata();
    std::vector<std::vector<uint8_t>> getSerPartitions(std::vector<uint8_t> topic_id) const;
    bool judgeTopicName(std::string name) const;
    std::vector<uint8_t> getTopicUuid(std::string topicN) const;
};
