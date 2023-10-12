#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <iostream>

#include "cmdline.h"
#include "log.hpp"

using namespace std;

int main(int argc, char **argv) {
    cmdline::parser cmd;
    cmd.add<std::string>("file");
    cmd.add<size_t>("size");
    bool ret = cmd.parse(argc, argv);
    if (!ret) {
        DLOG_FATAL("%s", cmd.error_full().c_str());
    }

    string file = cmd.get<string>("file");
    size_t size = cmd.get<size_t>("size");

    int fd = open(file.c_str(), O_RDWR | O_CREAT | O_SYNC, 0666);

    if (fd == -1) {
        DLOG_FATAL("");
    }

    char *addr = (char *)mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

    if (addr == nullptr) {
        DLOG_FATAL("");
    }

    if (addr == (void*)-1) {
        DLOG_FATAL("");
    }

    DLOG("mmap file: %p", addr);

    while (true) {
        cout << "$ ";
        string command;
        cin >> command;
        if (command == "q") {
            break;
        }
        if (command == "r") {
            int off;
            int size;
            cin >> off >> size;
            string str(addr + off, size);
            cout << str << endl;
        } else if (command == "w") {
            int off;
            string str;
            cin >> off;
            getline(cin, str);
            str = str.substr(1);
            memcpy(addr + off, str.c_str(), str.size());
        } else if (command == "s") {
            msync(addr, size, MS_SYNC);
        } else if (command == "h" || command == "?") {
            cout << "q\t\t\tQuit" << endl;
            cout << "r <off> <size>\t\t\tRead size bytes from off offset" << endl;
            cout << "w <off> <string>\t\t\tWrite string to off offset" << endl;
            cout << "s\t\t\tSave" << endl;
            cout << "h,?\t\t\tHelp" << endl;
        }
    }

    munmap(addr, size);
    close(fd);

    return 0;
}
