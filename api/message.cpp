#include "message.h"
#include <arpa/inet.h>
#include <cstring>

namespace ziplog {
namespace api {

    // https://linux.die.net/man/3/htonl
    // https://cplusplus.com/reference/vector/vector/insert/#google_vignette
    
    vector<uint8_t> message::serialize() const {
        vector<uint8_t> buffer;
        
        // validate message struct does not exceed desired size
        uint32_t struct_size = 4 + 4 + 4 + 4 + static_cast<uint32_t>(data.size());
        if (struct_size > MAX_MESSAGE_SIZE) return buffer;
       
        // add sender_id (4 bytes)
        uint32_t net_sender_id = htonl(sender_id);
        buffer.insert(buffer.end(),
                      reinterpret_cast<const uint8_t*>(&net_sender_id),
                      reinterpret_cast<const uint8_t*>(&net_sender_id) + 4);
        
        // add type (4 bytes)
        uint32_t net_type = htonl(static_cast<uint32_t>(type));
        buffer.insert(buffer.end(),
                      reinterpret_cast<const uint8_t*>(&net_type),
                      reinterpret_cast<const uint8_t*>(&net_type) + 4);
        
        // add sequence_number (4 bytes)
        uint32_t net_seq = htonl(sequence_number);
        buffer.insert(buffer.end(),
                      reinterpret_cast<const uint8_t*>(&net_seq),
                      reinterpret_cast<const uint8_t*>(&net_seq) + 4);
        
        // add data length (4 bytes)
        uint32_t data_len = htonl(static_cast<uint32_t>(data.length()));
        buffer.insert(buffer.end(),
                      reinterpret_cast<const uint8_t*>(&data_len),
                      reinterpret_cast<const uint8_t*>(&data_len) + 4);
        
        // add data
        buffer.insert(buffer.end(), data.begin(), data.end());
        
        return buffer;
    }
    
    std::optional<message> message::deserialize(const vector<uint8_t>& buffer) {
        if (buffer.size() < 16) {  // minimum size: 4 fields × 4 bytes each
            return std::nullopt;
        }
    
        message msg;
        size_t offset = 0;
        
        // read sender_id
        uint32_t net_sender_id;
        memcpy(&net_sender_id, buffer.data() + offset, 4);
        msg.sender_id = ntohl(net_sender_id);
        offset += 4;
        
        // read type
        uint32_t net_type;
        memcpy(&net_type, buffer.data() + offset, 4);
        msg.type = static_cast<messageType>(ntohl(net_type));
        offset += 4;
        
        // read sequence_number
        uint32_t net_seq;
        memcpy(&net_seq, buffer.data() + offset, 4);
        msg.sequence_number = ntohl(net_seq);
        offset += 4;
        
        // read data length
        uint32_t data_len;
        memcpy(&data_len, buffer.data() + offset, 4);
        data_len = ntohl(data_len);
        offset += 4;
        
        // validate data length
        if (offset + data_len > buffer.size()) {
            return std::nullopt;  // buffer too small for claimed data length
        }
        
        // read data
        msg.data = string(buffer.begin() + offset, buffer.begin() + offset + data_len);
        
        return msg;
    }
}}