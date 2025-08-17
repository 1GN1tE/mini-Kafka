#include "parseMetadata.hpp"
#include <cstdint>
#include <vector>
#include <fstream>
#include <cstring>
#include <cassert>
#include <iostream>
#include <set>
#include <map>
#include <string>
#include <arpa/inet.h>
#include <iostream>

parseMetadata::parseMetadata()
{
    std::ifstream file("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log", std::ios::binary);
    this->cluster_metadata.assign((std::istreambuf_iterator<char>(file)), {});

    // test
    //  std::cout << "cluster_metadata.size :" << cluster_metadata.size() << std::endl;

    getBatch_info();  // for std::map<int64_t, int32_t> batch_info(batch_index,batch_size)
    toRecords();      // for offsetToRec
    getRecords_num(); // how many records in each RecordBatch. for std::vector<int32_t> records_num

    parseTopic();
    parsePars();
}

int64_t parseMetadata::toBigEndian(int64_t littleEndianVal)
{
    uint64_t v = static_cast<uint64_t>(littleEndianVal);
    uint64_t result = ((v & 0x00000000000000FFULL) << 56) |
                      ((v & 0x000000000000FF00ULL) << 40) |
                      ((v & 0x0000000000FF0000ULL) << 24) |
                      ((v & 0x00000000FF000000ULL) << 8) |
                      ((v & 0x000000FF00000000ULL) >> 8) |
                      ((v & 0x0000FF0000000000ULL) >> 24) |
                      ((v & 0x00FF000000000000ULL) >> 40) |
                      ((v & 0xFF00000000000000ULL) >> 56);
    return static_cast<int64_t>(result);
}
int64_t parseMetadata::toLittleEndian(int64_t bigEndianVal)
{
    uint64_t v = static_cast<uint64_t>(bigEndianVal);
    uint64_t result = ((v & 0x00000000000000FFULL) << 56) |
                      ((v & 0x000000000000FF00ULL) << 40) |
                      ((v & 0x0000000000FF0000ULL) << 24) |
                      ((v & 0x00000000FF000000ULL) << 8) |
                      ((v & 0x000000FF00000000ULL) >> 8) |
                      ((v & 0x0000FF0000000000ULL) >> 24) |
                      ((v & 0x00FF000000000000ULL) >> 40) |
                      ((v & 0xFF00000000000000ULL) >> 56);
    return static_cast<int64_t>(result);
}
std::pair<int32_t, std::size_t> parseMetadata::readZigZagVarint(std::size_t offset)
{
    uint32_t raw = 0;
    int shift = 0;
    std::size_t bytesRead = 0;
    std::size_t bound = this->cluster_metadata.size();

    while (true)
    {
        if (offset + bytesRead >= bound)
        {
            throw std::runtime_error("readZigZagVarint: Out of bounds");
        }

        uint8_t byte = this->cluster_metadata[offset + bytesRead];
        raw |= (byte & 0x7f) << shift;
        bytesRead++;

        if ((byte & 0x80) == 0)
        {
            break;
        }

        shift += 7;
        if (shift > 35)
        {
            throw std::runtime_error("readZigZagVarint: Varint too long");
        }
    }

    int32_t value = (raw >> 1) ^ -(raw & 1);
    return {value, bytesRead};
}
std::pair<uint32_t, std::size_t> parseMetadata::readUnsignedVarint(std::size_t offset)
{
    uint32_t result = 0;
    int shift = 0;
    std::size_t bytesRead = 0;
    std::size_t bound = this->cluster_metadata.size();

    while (true)
    {
        if (offset + bytesRead >= bound)
        {
            throw std::runtime_error("Out of bounds");
        }

        uint8_t byte = this->cluster_metadata[offset + bytesRead];
        result |= (byte & 0x7f) << shift;
        bytesRead++;

        if ((byte & 0x80) == 0)
        {
            break;
        }

        shift += 7;
        if (shift >= 35)
        {
            throw std::runtime_error("Varint too long");
        }
    }

    return {result, bytesRead};
}

void parseMetadata::getBatch_info()
{
    std::size_t size = this->cluster_metadata.size() * sizeof(uint8_t);
    std::size_t offset = 0;
    int loop_cnt = 0;

    while (size > offset)
    {
        int64_t base_offset = 0;
        int32_t batch_len = 0;
        std::memcpy(&base_offset, this->cluster_metadata.data() + offset, sizeof(base_offset));
        offset += sizeof(base_offset);
        std::memcpy(&batch_len, this->cluster_metadata.data() + offset, sizeof(batch_len));
        offset += sizeof(batch_len);

        base_offset = toLittleEndian(base_offset);
        batch_len = ntohl(batch_len);

        // test
        //  std::cout << "loop: " << loop_cnt << "| base_offset: " << base_offset
        //            << "| batch_len: " << batch_len << std::endl;

        // //assert
        // if (loop_cnt == 0) {
        //     assert(base_offset == 1 && "First batch must have base_offset == 1");
        // } else if (loop_cnt == 1) {
        //     assert(base_offset == 2 && "Second batch must have base_offset == 2");
        // }

        this->batch_info[base_offset] = batch_len;
        offset += batch_len;
        loop_cnt++;
    }

    // std::cout << "batch_info.size:" << batch_info.size() << std::endl;
}
void parseMetadata::toRecords()
{
    int64_t base_offset;
    int32_t batch_len;
    int32_t leader_epoch;
    uint8_t batch_format;

    int32_t crc;
    int16_t attributes;
    int32_t last_offset;
    int64_t base_timestamp;

    int64_t max_timestamp;
    int64_t producer_id;
    int16_t producer_epoch;
    int32_t base_sequence;

    int32_t records_len;

    offsetToRec = offsetToRec + sizeof(base_offset) + sizeof(batch_len) + sizeof(leader_epoch) + sizeof(batch_format);
    offsetToRec = offsetToRec + sizeof(crc) + sizeof(attributes) + sizeof(last_offset) + sizeof(base_timestamp);
    offsetToRec = offsetToRec + sizeof(max_timestamp) + sizeof(producer_id) + sizeof(producer_epoch) + sizeof(base_sequence);
    offsetToRec += sizeof(records_len);

    // std::cout << "In toRecords(),  toRecords: " << this->offsetToRec << std::endl;
}
void parseMetadata::getRecords_num()
{
    // test
    //  std::cout << "toRecords: " << this->offsetToRec << std::endl;

    int32_t record_num = 0;
    std::size_t toRec_num = this->offsetToRec - sizeof(record_num);
    std::size_t offset = 0;

    for (const auto &[base_offset, batch_len] : batch_info)
    {
        record_num = 0;

        offset += toRec_num;
        std::memcpy(&record_num, this->cluster_metadata.data() + offset, sizeof(record_num));
        record_num = ntohl(record_num);
        this->records_num.push_back(record_num);

        offset -= toRec_num;
        offset += sizeof(base_offset); // 一开始代码没有offset += sizeof(base_offset);和offset += sizeof(batch_len);
        offset += sizeof(batch_len);   // 以为只要offset -= toRec_num;和offset += batch_len;两步便能正确跳到下一个record_batch
        offset += batch_len;           // 导致解析出错误的rec_num，产生-1、756350976、0 等奇怪值，影响构造函数中的循环不能正确运行

        // test
        //  std::cout << "In getRecords_num(), sizeof(base_offset) is :" << sizeof(base_offset) << std::endl;
        //  std::cout << "sizeof(batch_len) is :" << sizeof(batch_len) << std::endl;
    }

    // test
    //  std::cout << "In getRecords_num(), records_num (" << records_num.size() << " bytes): ";
    //  for (int32_t num : records_num) {
    //      std::cout << num << std::endl;
    //  }
    //  std::cout << "edge: In getRecords_num();---------------------------------" << std::endl;
}
std::size_t parseMetadata::getRecToValue(std::size_t offset)
{
    std::size_t recToValue = 0; // 之前未将recToValue进行初始化，出现一个很奇怪的数字
    std::size_t ptr = offset;

    auto [len, len_size] = readZigZagVarint(ptr);
    recToValue += len_size;
    ptr += len_size;

    uint8_t attributes_;
    recToValue += sizeof(attributes_);
    ptr += sizeof(attributes_);

    auto [timestamp_delta, size] = readZigZagVarint(ptr);
    recToValue += size;
    ptr += size;

    auto [offset_delta, od_size] = readZigZagVarint(ptr);
    recToValue += od_size;
    ptr += od_size;

    auto [key_len, kl_size] = readZigZagVarint(ptr);
    // if (key_len == -1) {
    //     std::cout << "the key is null(In getRecToValue)" << std::endl;
    // }
    recToValue += kl_size;
    ptr += kl_size;

    auto [value_len, vl_size] = readZigZagVarint(ptr);
    recToValue += vl_size;
    ptr += vl_size;

    // test
    //  std::cout << "In getRecToValue, len is: " << len;
    //  std::cout << "len_size is: " << len_size << std::endl;

    // std::cout << "In getRecToValue, timestamp_delta is: " << timestamp_delta;
    // std::cout << "timestamp_delta_size is: " << size << std::endl;

    // std::cout << "In getRecToValue, offset_delta is: " << offset_delta;
    // std::cout << "od_size is: " << od_size << std::endl;

    // std::cout << "In getRecToValue, key_len is: " << key_len;
    // std::cout << "kl_size is: " << kl_size << std::endl;

    // std::cout << "In getRecToValue, value_len is: " << value_len;
    // std::cout << "vl_size is: " << vl_size << std::endl;

    return recToValue;
}
std::size_t parseMetadata::toNextBatch(std::size_t offset)
{
    std::size_t beginOff = sizeof(int64_t) + sizeof(int32_t);
    std::size_t toNext = 0;

    for (auto &[base_offset, batch_len] : batch_info)
    {
        if (offset > toNext)
        {
            toNext = toNext + beginOff + batch_len;
        }
    }

    return toNext;
}

void parseMetadata::parseTopicHelper(std::size_t offset)
{
    uint8_t ver;
    offset = offset + sizeof(ver);

    auto [nLen, nLen_size] = readUnsignedVarint(offset);
    offset += nLen_size;

    std::size_t len = nLen - 1;
    std::string name;
    name.assign(reinterpret_cast<const char *>(this->cluster_metadata.data() + offset), len);
    this->topic_name.insert(name);
    offset += len;

    std::vector<uint8_t> uuid(cluster_metadata.begin() + offset, cluster_metadata.begin() + offset + 16);
    nameToTopicId[name] = uuid;
    offset = offset + sizeof(uint8_t) * 16;
}
void parseMetadata::parseTopic()
{
    std::size_t main_offset = 0;
    std::size_t size = this->cluster_metadata.size() * sizeof(uint8_t);
    int loop = 0;
    std::size_t i_offset = 0;
    std::size_t index = 0;
    int32_t rec_num = 0;

    while (size > main_offset)
    {
        main_offset += this->offsetToRec;
        rec_num = records_num[loop];
        i_offset = main_offset;

        for (int i = 0; i < rec_num; i++)
        {
            auto [rec_len, varint_bytes] = readZigZagVarint(i_offset);

            index = getRecToValue(i_offset);
            i_offset += index;

            uint8_t frame_version;
            i_offset += sizeof(frame_version);
            index += sizeof(frame_version);

            uint8_t type = cluster_metadata[i_offset];
            i_offset += sizeof(type);
            index += sizeof(type);
            if (type == 2)
            {
                parseTopicHelper(i_offset);
            }

            i_offset -= index;
            i_offset = i_offset + rec_len + varint_bytes;
        }

        main_offset = toNextBatch(main_offset);
        loop++;
    }
}

void parseMetadata::topicMatchPars(std::vector<uint8_t> uuid, std::vector<uint8_t> par)
{
    this->topicToPars[uuid].push_back(par);
}

std::vector<uint8_t> parseMetadata::parseParsHelper(std::size_t offset)
{
    uint8_t ver;
    offset += sizeof(ver);

    int32_t partition_id;
    std::memcpy(&partition_id, cluster_metadata.data() + offset, sizeof(partition_id));
    offset += sizeof(partition_id);

    std::vector<uint8_t> topic_uuid;
    topic_uuid.assign(cluster_metadata.begin() + offset, cluster_metadata.begin() + offset + 16);
    std::size_t id_size = topic_uuid.size() * sizeof(uint8_t);
    offset += id_size;

    auto [replicaArray_len, size] = readUnsignedVarint(offset);
    offset += size;

    std::vector<int32_t> replica_array;
    int r_len = replicaArray_len - 1;
    replica_array.resize(r_len);
    std::memcpy(replica_array.data(), cluster_metadata.data() + offset, sizeof(int32_t) * r_len);
    std::size_t t = sizeof(int32_t) * r_len;
    offset += t;

    auto [isrArray_len, isr_size] = readUnsignedVarint(offset);
    offset += isr_size;

    std::vector<int32_t> isr_array;
    int isr_len = isrArray_len - 1;
    isr_array.resize(isr_len);
    std::memcpy(isr_array.data(), cluster_metadata.data() + offset, sizeof(int32_t) * isr_len);
    std::size_t i = sizeof(int32_t) * isr_len;
    offset += i;

    auto [rrArray_len, rr_size] = readUnsignedVarint(offset);
    offset += rr_size;

    auto [arArray_len, ar_size] = readUnsignedVarint(offset);
    offset += ar_size;

    int32_t leader;
    std::memcpy(&leader, cluster_metadata.data() + offset, sizeof(leader));
    offset += sizeof(leader);

    int32_t leader_epoch;
    std::memcpy(&leader_epoch, cluster_metadata.data() + offset, sizeof(leader_epoch));

    std::vector<uint8_t> sp;

    int16_t error_code = 0;
    uint8_t elr_len = 1;
    uint8_t lk_elr_len = 1;
    uint8_t or_nodes_len = 1;
    uint8_t tag_buf = 0;

    sp.insert(sp.end(), reinterpret_cast<uint8_t *>(&error_code), reinterpret_cast<uint8_t *>(&error_code) + sizeof(error_code));
    sp.insert(sp.end(), reinterpret_cast<uint8_t *>(&partition_id), reinterpret_cast<uint8_t *>(&partition_id) + sizeof(partition_id));
    sp.insert(sp.end(), reinterpret_cast<uint8_t *>(&leader), reinterpret_cast<uint8_t *>(&leader) + sizeof(leader));
    sp.insert(sp.end(), reinterpret_cast<uint8_t *>(&leader_epoch), reinterpret_cast<uint8_t *>(&leader_epoch) + sizeof(leader_epoch));
    sp.push_back(replicaArray_len);

    std::size_t ra_size = replica_array.size() * sizeof(int32_t);
    sp.insert(sp.end(), reinterpret_cast<uint8_t *>(replica_array.data()), reinterpret_cast<uint8_t *>(replica_array.data()) + ra_size);
    sp.push_back(isrArray_len);

    std::size_t ia_size = isr_array.size() * sizeof(int32_t);
    sp.insert(sp.end(), reinterpret_cast<uint8_t *>(isr_array.data()), reinterpret_cast<uint8_t *>(isr_array.data()) + ia_size);

    sp.push_back(elr_len);
    sp.push_back(lk_elr_len);
    sp.push_back(or_nodes_len);
    sp.push_back(tag_buf);

    topicMatchPars(topic_uuid, sp);

    return sp;
}
void parseMetadata::parsePars()
{
    std::size_t main_offset = 0;
    std::size_t size = this->cluster_metadata.size() * sizeof(uint8_t);
    int loop = 0;
    std::size_t i_offset = 0;
    std::size_t index = 0;
    int32_t rec_num = 0;

    while (size > main_offset)
    {
        main_offset += this->offsetToRec;
        rec_num = records_num[loop];
        i_offset = main_offset;

        int t = 0;

        for (int i = 0; i < rec_num; i++)
        {
            // std::cout << "[debug, in parsePars(); for() ]" << " rec_num is : " << rec_num << std::endl;

            auto [rec_len, varint_bytes] = readZigZagVarint(i_offset);

            index = getRecToValue(i_offset);
            i_offset += index;

            uint8_t frame_version;
            i_offset += sizeof(frame_version);
            index += sizeof(frame_version);

            uint8_t type = cluster_metadata[i_offset];
            // std::cout << "[debug, in parsePars();] " << " type = " << (int)type << " at offset=" << i_offset << std::endl;

            i_offset += sizeof(type);
            index += sizeof(type);
            if (type == 3)
            {
                t++;

                // std::cout << "parse valParType successfully in parsePars();" << std::endl;
                partitions.push_back(parseParsHelper(i_offset));
            }

            i_offset -= index;
            i_offset = i_offset + rec_len + varint_bytes;
        }

        // test
        //  std::cout << "--------------------In parsePars(), each Batch have " << t << " times in if (type == 3) -------------------------" << std::endl;

        main_offset = toNextBatch(main_offset);
        loop++;
    }
}

std::vector<std::vector<uint8_t>> parseMetadata::getSerPartitions(std::vector<uint8_t> topic_id) const
{
    // test
    //  if (partitions.empty()) {
    //      std::cout << "partitions is empty" << std::endl;
    //  } else {
    //      std::cout << "partitions is not empty" << std::endl;
    //  }

    // if (topicToPars.empty()) {
    //     std::cout << "topicToPars is empty" << std::endl;
    // } else {
    //     std::cout << "topicToPars is not empty" << std::endl;
    // }

    auto it = topicToPars.find(topic_id);
    if (it != topicToPars.end())
    {
        return it->second;
    }

    return {};
}
bool parseMetadata::judgeTopicName(std::string name) const
{
    // test
    //  if (topic_name.empty()) {
    //      std::cout << "[debug] topic_name is empty!" << std::endl;
    //  } else {
    //      std::cout << "[debug] topic_name is not empty!" << std::endl;
    //      std::cout << "[debug] topic_name contains topics:\n";
    //      for (const auto& name : topic_name) {
    //          std::cout << " - " << name << std::endl;
    //      }
    //  }

    if (topic_name.find(name) != topic_name.end())
    {
        // test
        //  std::cout << "In bool parseMetadata::judgeTopicName(std::string name); topic_name is exit " << std::endl;

        return true;
    }
    else
    {
        // test
        //  std::cout << "In bool parseMetadata::judgeTopicName(std::string name); topic_name is not exit!!!! " << std::endl;

        return false;
    }
}
std::vector<uint8_t> parseMetadata::getTopicUuid(std::string topicN) const
{
    // test
    //  if (nameToTopicId.empty()) {
    //      std::cout << "nameToTopicId is empty" << std::endl;
    //  } else {
    //      std::cout << "nameToTopicId is not empty" << std::endl;
    //      for (const auto& [name, topicId] : nameToTopicId) {
    //          std::cout << "Name: " << name << " -> TopicId bytes: ";
    //          for (uint8_t byte : topicId) {
    //              printf("%02X ", byte);
    //          }
    //          std::cout << std::endl;
    //      }

    // }

    // std::cout << "********************************************************" << std::endl;

    // test
    //  for (const auto& [topicId, partitions] : topicToPars) {
    //      // 打印 topicId
    //      std::cout << "TopicId bytes: ";
    //      for (uint8_t byte : topicId) {
    //          printf("%02X ", byte);
    //      }
    //      std::cout << " -> Partitions:" << std::endl;

    //     // 打印每个 partition
    //     for (size_t i = 0; i < partitions.size(); ++i) {
    //         std::cout << "  Partition " << i << ": ";
    //         for (uint8_t byte : partitions[i]) {
    //             printf("%02X ", byte);
    //         }
    //         std::cout << std::endl;
    //     }
    // }

    auto it = nameToTopicId.find(topicN);
    if (it != nameToTopicId.end())
    {
        return it->second;
    }
    else
    {
        std::vector<uint8_t> unknown{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        return unknown;
    }
}
