#include <cassert>
#include <cstdint>
#include <thread>

#include "cmdline.h"
#include "common_sta.h"
#include "rdma_rc.hpp"

using namespace erpc;

#if ONESIDE_EXT == 1
rdma_rc::RDMAConnection conn;
uintptr_t server_mr_addr;
uint32_t server_mr_rkey;
uintptr_t mr_addr = 0;
uint32_t mr_lkey = 0;
#endif

void a_plus_b(void *_c, void *_tag) {
    auto c = reinterpret_cast<AppContext *>(_c);
    auto tag = reinterpret_cast<size_t>(_tag);

    auto req_raw = c->reqs[tag];
    auto resp_raw = c->resps[tag];

    auto resp = reinterpret_cast<APlusBResp *>(resp_raw.get_buf());
    auto req = reinterpret_cast<APlusBReq *>(req_raw.get_buf());
    if (req->a + req->b != resp->result) {
        printf("error %d + %d != %d\n", req->a, req->b, resp->result);
        abort();
        // ERPC_ERROR("error %d + %d != %d", req->a, req->b, resp->result);
    }
    c->cnt--;
}

void cb(void *_c, void *_tag) {
    auto c = reinterpret_cast<AppContext *>(_c);
    c->cnt--;
}

void thread_func(Params param, erpc::NexusWrap *nexus, Stat *stat, int thread_idx) {
    AppContext ctx;
    FastRand rd;

    size_t resp_s = param.payload;
#if ONESIDE_EXT == 1
    resp_s = 8;
    std::vector<rdma_rc::SgeWr> sge_wrs;
#endif

    erpc::SMHandlerWrap smhw;
    smhw.set_empty();
    auto rpc = new erpc::IBRpcWrap(nexus, &ctx, thread_idx, smhw);

    std::string server_uri = kServerHostname + ":" + std::to_string(kServerUDPPort);
    std::vector<int> sess_vec;
    for (size_t i = 0; i < param.sess_num; i++) {
        int session_num = rpc->create_session(server_uri, 0);

        sess_vec.push_back(session_num);
    }

    ctx.reqs.reserve(param.op_per_thread);
    ctx.resps.reserve(param.op_per_thread);
    ctx.cnt = param.op_per_thread;
    ChronoTimer timer;

    auto st = timer.get_us();
    for (size_t i = 0; i < param.op_per_thread; i += param.batch_size) {
        ctx.cnt = param.batch_size;
        // for (size_t j = 0; j < param.batch_size; j++) {
        //     auto req = rpc->alloc_msg_buffer_or_die(64);
        //     auto resp = rpc->alloc_msg_buffer_or_die(resp_s);
        //     SimpleDataReq *r = (SimpleDataReq *)req.get_buf();
        //     r->s = resp_s;
        //     ctx.reqs.push_back(req);
        //     ctx.resps.push_back(resp);
        //     rpc->enqueue_request(sess_vec[rd.next_u32() % param.sess_num], kSimpleReqType,
        //                          ctx.reqs[i + j], ctx.resps[i + j], cb,
        //                          reinterpret_cast<void *>(i + j));
        // }
        // while (ctx.cnt != 0) rpc->run_event_loop_once();

        // for (size_t j = 0; j < param.batch_size; j++) {
        //     rpc->free_msg_buffer(ctx.reqs[i + j]);
        //     rpc->free_msg_buffer(ctx.resps[i + j]);
        // }

#if ONESIDE_EXT == 1
        conn.prep_write(sge_wrs, mr_addr, mr_lkey, param.payload, server_mr_addr, server_mr_rkey, false);
        rdma_rc::RDMAFuture fu = conn.submit(sge_wrs);
        fu.get();
#endif

    }
    auto ed = timer.get_us();
    stat->elapse += (ed - st);

    // auto req = rpc->alloc_msg_buffer_or_die(kMsgSize);
    // auto resp = rpc->alloc_msg_buffer_or_die(kMsgSize);
    // rpc->enqueue_request(session_num, kStopType, &req, &resp, nullptr,
    // nullptr); rpc->run_event_loop(200);

    delete rpc;
}

int main(int argc, char **argv) {
    cmdline::parser cmd;
    cmd.add<int>("thread", 't', "thread num", true);
    cmd.add<size_t>("batch", 'b', "batch size", false, 10);
    cmd.add<size_t>("op", 0, "operation num", true);
    cmd.add<size_t>("session", 's', "session number of each rpc", false, 1);
    cmd.add<size_t>("payload");

#if ONESIDE_EXT == 1
    cmd.add<std::string>("server_ip");
    cmd.add<uint16_t>("server_port");
    cmd.add<uintptr_t>("mr_addr", 0, "", false);
    cmd.add<uint32_t>("mr_rkey", 0, "", false);
#endif

    cmd.parse_check(argc, argv);

#if ONESIDE_EXT == 1
    rdma_rc::RDMAEnv::init();
    conn.connect(cmd.get<std::string>("server_ip"), cmd.get<uint16_t>("server_port"), nullptr, 0);

    ibv_mr *mr = conn.register_memory(1ul << 20);
    if (mr == nullptr) abort();
    mr_addr = (uintptr_t)mr->addr;
    mr_lkey = mr->lkey;

    server_mr_addr = cmd.get<uintptr_t>("mr_addr");
    server_mr_rkey = cmd.get<uint32_t>("mr_rkey");
#endif

    auto thread_num = cmd.get<int>("thread");
    auto bs = cmd.get<size_t>("batch");
    auto op = cmd.get<size_t>("op");
    auto sess = cmd.get<size_t>("session");
    auto payload = cmd.get<size_t>("payload");

    std::string client_uri = kClientHostname + ":" + std::to_string(kClientUDPPort);
    erpc::NexusWrap nexus(client_uri);

    Params param{
        thread_num, op, bs, sess, payload,
    };
    Stat stat;
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_num; i++) {
        threads.emplace_back(thread_func, param, &nexus, &stat, i);
    }

    for (auto &th : threads) {
        th.join();
    }

    auto avg = stat.elapse * 1.0 / param.thread_num;
    auto tput = param.op_per_thread * 1.0 * thread_num / (avg * 1.0);

    printf("Throughput %lf MOps\n", tput);
}
