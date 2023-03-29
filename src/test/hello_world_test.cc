#include <string>
#include <cstring>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <vector>
#include <iostream>

using namespace std;

int main()
{
    struct ifaddrs *ifaddr, *ifa;
    int family, s;
    char host[NI_MAXHOST];

    if (getifaddrs(&ifaddr) == -1) {
        std::cerr << "getifaddrs failed" << std::endl;
        return 1;
    }

    std::vector<std::string> ipAddresses;

    for (ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == nullptr) {
            continue;
        }

        family = ifa->ifa_addr->sa_family;

        if (family == AF_INET || family == AF_INET6) {
            s = getnameinfo(ifa->ifa_addr,
                            (family == AF_INET) ? sizeof(struct sockaddr_in) :
                            sizeof(struct sockaddr_in6),
                            host, NI_MAXHOST,
                            nullptr, 0, NI_NUMERICHOST);
            if (s != 0) {
                std::cerr << "getnameinfo failed: " << gai_strerror(s) << std::endl;
                return 1;
            }

            if (family == AF_INET) {
                ipAddresses.push_back(host);
            }
        }
    }

    freeifaddrs(ifaddr);

    std::cout << "Local IP addresses:" << std::endl;
    for (const auto& address : ipAddresses) {
        std::cout << address << std::endl;
    }

    return 0;
}