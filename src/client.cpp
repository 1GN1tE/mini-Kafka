#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <vector>

// Convert hex string to raw bytes
std::vector<unsigned char> hexToBytes(const std::string &hex) {
    std::vector<unsigned char> bytes;
    for (size_t i = 0; i < hex.length(); i += 2) {
        std::string byteString = hex.substr(i, 2);
        unsigned char byte = static_cast<unsigned char>(strtol(byteString.c_str(), nullptr, 16));
        bytes.push_back(byte);
    }
    return bytes;
}

// Print data in hex, like hexdump -C
void printHex(const unsigned char* data, size_t len) {
    for (size_t i = 0; i < len; i++) {
        printf("%02x ", data[i]);
        if ((i + 1) % 16 == 0) printf("\n");
    }
    if (len % 16 != 0) printf("\n");
}

int main() {
    const std::string hexData =
        "00000023001200046f7fc66100096b61666b612d636c69000a6b61666b612d636c6904302e3100";

    // Convert hex to raw binary
    std::vector<unsigned char> data = hexToBytes(hexData);

    // Create socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Socket creation failed");
        return 1;
    }

    // Connect to localhost:9092
    sockaddr_in server{};
    server.sin_family = AF_INET;
    server.sin_port = htons(9092);
    inet_pton(AF_INET, "127.0.0.1", &server.sin_addr);

    if (connect(sock, (struct sockaddr*)&server, sizeof(server)) < 0) {
        perror("Connect failed");
        close(sock);
        return 1;
    }

    // Send message 4 times and read each response
    for (int i = 0; i < 4; i++) {
        ssize_t sent = send(sock, data.data(), data.size(), 0);
        if (sent < 0) {
            perror("Send failed");
            close(sock);
            return 1;
        }
        std::cout << "Sent message " << (i + 1) << std::endl;

        unsigned char buffer[1024];
        ssize_t n = recv(sock, buffer, sizeof(buffer), 0);
        if (n > 0) {
            std::cout << "Response " << (i + 1) << ": " << n << " bytes\n";
            printHex(buffer, n);
        } else if (n == 0) {
            std::cout << "Server closed connection\n";
            break;
        } else {
            perror("Recv failed");
            break;
        }
    }

    close(sock);
    return 0;
}
