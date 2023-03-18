#pragma once

#include <string>

namespace rchms {

enum Status {
    ERROR = 0,
    OK = 1,
};

inline static std::string GetStatusString(Status s) {
    switch (s) {
        case ERROR:
            return "ERROR";
        case OK:
            return "OK";
        default:
            return "Unkown Status";
    }
}

}  // namespace rchms