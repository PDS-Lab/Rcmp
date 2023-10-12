#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif

#define FUSE_USE_VERSION 39

#include <asm-generic/errno-base.h>
#include <fcntl.h>
#include <fuse3/fuse.h>
#include <sys/stat.h>

#include <algorithm>
#include <cstring>
#include <iostream>
#include <list>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "config.hpp"
#include "log.hpp"
#include "rcmp.hpp"
#include "utils.hpp"

using namespace std;

struct Entry;

using EntryPair = pair<const string, Entry>;

struct Entry {
    struct stat st;
    list<EntryPair*> subfile_list;
    vector<rcmp::GAddr> block_map;
};

using UMITER = typename unordered_map<string, Entry>::iterator;

rcmp::ClientOptions options;
unordered_map<string, Entry> file_map;
rcmp::PoolContext* pool;

string getname(const string& path) {
    size_t pos = path.find_last_of('/');
    if (pos == path.npos) {
        return "";
    }
    return path.substr(pos + 1);
}

string getdirname(const string& path) {
    string dir = path.substr(0, path.find_last_of('/'));
    if (dir.empty()) {
        dir = "/";
    }
    return dir;
}

void dir_add_subfile(UMITER fileit) {
    string dirpath_sv = getdirname(fileit->first);
    auto dirit = file_map.find(dirpath_sv);
    dirit->second.subfile_list.push_back(&(*fileit));
}

void dir_remove_subfile(UMITER fileit) {
    string dirpath_sv = getdirname(fileit->first);
    auto dirit = file_map.find(dirpath_sv);
    auto subfile_it = find_if(dirit->second.subfile_list.begin(), dirit->second.subfile_list.end(),
                              [&](EntryPair* p) { return fileit->first == p->first; });
    dirit->second.subfile_list.erase(subfile_it);
}

bool try_reserve(Entry& ent, off_t size) {
    off_t minsize = align_ceil(size, page_size);
    size_t reserve_size = ent.block_map.size() * page_size;
    if (minsize > reserve_size) {
        // alloc allocsize bytes, aligned to page_size
        size_t allocsize = minsize - reserve_size;
        size_t alloc_page_cnt = allocsize / page_size;
        rcmp::GAddr alloc_start_gaddr = pool->AllocPage(alloc_page_cnt);
        for (size_t i = 0; i < alloc_page_cnt; ++i) {
            ent.block_map.push_back(alloc_start_gaddr + i * page_size);
        }
        return true;
    }
    return false;
}

UMITER createdir(const char* path, mode_t mode) {
    Entry new_entry = {.st = {
                           .st_nlink = 2,
                           .st_mode = S_IFDIR | mode,
                       }};
    auto it = file_map.insert({path, new_entry}).first;
    dir_add_subfile(it);
    return it;
}

UMITER createfile(const char* path, mode_t mode, dev_t rdev) {
    Entry new_entry = {.st = {
                           .st_nlink = 1,
                           .st_mode = S_IFREG | mode,
                       }};
    auto it = file_map.insert({path, new_entry}).first;
    dir_add_subfile(it);
    return it;
}

void* rchfs_init(struct fuse_conn_info* conn, struct fuse_config* cfg) {
    DLOG("");
    pool = rcmp::Open(options);
    file_map.insert({"/", (Entry){.st = {
                                      .st_nlink = 2,
                                      .st_mode = 0775 | S_IFDIR,
                                  }}});
    return nullptr;
}

void rchfs_destroy(void* ctx) {
    DLOG("");
    rcmp::Close(pool);
}

int rchfs_getattr(const char* path, struct stat* stbuf, struct fuse_file_info* fi) {
    DLOG("path: %s", path);

    Entry* ent;
    if (fi == nullptr || (ent = (Entry*)fi->fh) == nullptr) {
        auto it = file_map.find(path);
        if (it == file_map.end()) {
            return -ENOENT;
        }
        ent = &it->second;
    }
    *stbuf = ent->st;
    return 0;
}

int rchfs_mknod(const char* path, mode_t mode, dev_t rdev) {
    DLOG("path: %s", path);

    auto it = file_map.find(path);
    if (it != file_map.end()) {
        return -EEXIST;
    }
    createfile(path, mode, rdev);
    return 0;
}

int rchfs_mkdir(const char* path, mode_t mode) {
    DLOG("path: %s", path);

    auto it = file_map.find(path);
    if (it != file_map.end()) {
        return -EEXIST;
    }
    createdir(path, mode);
    return 0;
}

int rchfs_readdir(const char* path, void* buf, fuse_fill_dir_t filler, off_t off,
                  struct fuse_file_info* fi, enum fuse_readdir_flags) {
    DLOG("path: %s", path);

    Entry* ent = (Entry*)fi->fh;
    if (ent == nullptr) {
        auto it = file_map.find(path);
        if (it == file_map.end()) {
            return -ENOENT;
        }
        ent = &it->second;
    }
    for (auto& p : ent->subfile_list) {
        filler(buf, getname(p->first).c_str(), &ent->st, 0, FUSE_FILL_DIR_PLUS);
    }
    return 0;
}

int rchfs_open(const char* path, struct fuse_file_info* fi) {
    DLOG("path: %s", path);

    auto it = file_map.find(path);
    if (it == file_map.end()) {
        it = createfile(path, 0664, 0);
    }
    fi->fh = (uint64_t)&it->second;
    return 0;
}

int rchfs_read(const char* path, char* buf, size_t size, off_t off, struct fuse_file_info* fi) {
    DLOG("path: %s, size: %lu", path, size);

    Entry* ent = (Entry*)fi->fh;
    if (off > ent->st.st_size) {
        return 0;
    }
    size_t avail = ent->st.st_size - off;
    size_t rsize = (size < avail) ? size : avail;
    size = rsize;

    // read off rsize bytes
    size_t bi = div_floor(off, page_size);
    while (rsize > 0) {
        rcmp::GAddr read_addr = ent->block_map[bi];
        size_t s = min(rsize, align_ceil((size_t)(off + 1), page_size) - off);
        pool->Read(read_addr + off - align_floor(off, page_size), s, buf);

        off += s;
        rsize -= s;
        buf += s;
        ++bi;
    }
    return size;
}

int rchfs_write(const char* path, const char* buf, size_t size, off_t off,
                struct fuse_file_info* fi) {
    DLOG("path: %s, size: %lu", path, size);

    Entry* ent = (Entry*)fi->fh;
    off_t minsize = off + size;
    if (minsize > ent->st.st_size) {
        ent->st.st_size = minsize;
    }
    try_reserve(*ent, minsize);

    // write off size bytes
    size_t bi = div_floor(off, page_size);
    size_t rsize = size;
    while (rsize > 0) {
        rcmp::GAddr write_addr = ent->block_map[bi];
        size_t s = min(rsize, align_ceil((size_t)(off + 1), page_size) - off);
        pool->Write(write_addr + off - align_floor(off, page_size), s, buf);

        off += s;
        rsize -= s;
        buf += s;
        ++bi;
    }
    return size;
}

int rchfs_truncate(const char* path, off_t size, struct fuse_file_info* fi) {
    DLOG("path: %s", path);

    auto it = file_map.find(path);
    Entry& ent = it->second;
    ent.st.st_size = size;
    try_reserve(ent, size);
    return 0;
}

int rchfs_unlink(const char* path) {
    DLOG("path: %s", path);

    auto it = file_map.find(path);
    dir_remove_subfile(it);
    for (auto addr : it->second.block_map) {
        pool->FreePage(addr, 1);
    }
    file_map.erase(it);
    return 0;
}

int rchfs_rmdir(const char* path) {
    DLOG("path: %s", path);

    auto it = file_map.find(path);
    dir_remove_subfile(it);
    file_map.erase(it);
    return 0;
}

struct fuse_operations rchfs_ops = {
    .getattr = rchfs_getattr,
    .mknod = rchfs_mknod,
    .mkdir = rchfs_mkdir,
    .unlink = rchfs_unlink,
    .rmdir = rchfs_rmdir,
    .truncate = rchfs_truncate,
    .open = rchfs_open,
    .read = rchfs_read,
    .write = rchfs_write,
    .readdir = rchfs_readdir,
    .init = rchfs_init,
    .destroy = rchfs_destroy,
};

struct option {
    char* client_ip;
    uint16_t client_port;

    uint32_t rack_id;

    bool with_cxl = true;
    char* cxl_devdax_path;
    size_t cxl_memory_size;
    int prealloc_fiber_num = 2;
};

#define OPTION(t, p) \
    { t, offsetof(option, p), 1 }

const fuse_opt option_spec[] = {OPTION("--client_ip=%s", client_ip),
                                OPTION("--client_port=%hu", client_port),
                                OPTION("--cxl_devdax_path=%s", cxl_devdax_path),
                                OPTION("--cxl_memory_size=%lu", cxl_memory_size),
                                OPTION("--rack_id=%u", rack_id),
                                FUSE_OPT_END};

int main(int argc, char* argv[]) {
    int ret;
    option opts{};
    fuse_args args = FUSE_ARGS_INIT(argc, argv);

    if (fuse_opt_parse(&args, &opts, option_spec, NULL) == -1) return 1;

    options.client_ip = opts.client_ip;
    options.client_port = opts.client_port;
    options.rack_id = opts.rack_id;
    options.with_cxl = opts.with_cxl;
    options.cxl_devdax_path = opts.cxl_devdax_path;
    options.cxl_memory_size = opts.cxl_memory_size;
    options.prealloc_fiber_num = opts.prealloc_fiber_num;

    ret = fuse_main(args.argc, args.argv, &rchfs_ops, NULL);
    fuse_opt_free_args(&args);
    return ret;
}