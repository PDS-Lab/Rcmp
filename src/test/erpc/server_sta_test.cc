#include <cstdint>
#include <thread>

#include "cmdline.h"
#include "common_sta.h"
#include "rdma_rc.hpp"

using namespace std;

void req_handler(erpc::ReqHandle *req_handle, void *_c) {
  auto ctx = reinterpret_cast<ServerContext *>(_c);
  erpc::ReqHandleWrap req_wrap(req_handle);
  auto resp_raw = req_wrap.get_pre_resp_msgbuf();
  auto req_raw = req_wrap.get_req_msgbuf();

  auto req = reinterpret_cast<APlusBReq *>(req_raw.get_buf());
  auto resp = reinterpret_cast<APlusBResp *>(resp_raw.get_buf());
  resp->result = req->a + req->b;

  ctx->rpc->resize_msg_buffer(resp_raw, kMsgSize);
  ctx->rpc->enqueue_response(req_wrap, resp_raw);
}

void req_handler2(erpc::ReqHandle *req_handle, void *_c) {
  auto ctx = reinterpret_cast<ServerContext *>(_c);
  erpc::ReqHandleWrap req_wrap(req_handle);
  auto req_raw = req_wrap.get_req_msgbuf();
  auto req = reinterpret_cast<APlusBReq *>(req_raw.get_buf());

  auto resp_msg_buf = req_wrap.get_dyn_resp_msgbuf();
  resp_msg_buf.set(ctx->rpc->alloc_msg_buffer_or_die(kMsgSize));
  auto resp = reinterpret_cast<APlusBResp *>(resp_msg_buf.get_buf());

  resp->result = req->a + req->b;

  ctx->rpc->enqueue_response(req_wrap, resp_msg_buf);
}

void simple_data_handler(erpc::ReqHandle *req_handle, void *_c) {
  auto ctx = reinterpret_cast<ServerContext *>(_c);
  erpc::ReqHandleWrap req_wrap(req_handle);
  auto req_raw = req_wrap.get_req_msgbuf();
  auto req = reinterpret_cast<SimpleDataReq *>(req_raw.get_buf());

  auto resp_msg_buf = req_wrap.get_dyn_resp_msgbuf();
  resp_msg_buf.set(ctx->rpc->alloc_msg_buffer_or_die(req->s));
  auto resp = reinterpret_cast<SimpleDataResp *>(resp_msg_buf.get_buf());

  ctx->rpc->enqueue_response(req_wrap, resp_msg_buf);
}

void stop(erpc::ReqHandle *, void *_c) {
  auto ctx = reinterpret_cast<ServerContext *>(_c);
  ctx->running = false;
  printf("stop %d \n", ctx->thread_idx);
}

void server_func(erpc::NexusWrap *nexus, int thread_idx) {
  ServerContext ctx;
  ctx.running = true;
  ctx.thread_idx = thread_idx;

  erpc::SMHandlerWrap smhw;
  smhw.set_null();

  auto rpc = new erpc::IBRpcWrap(nexus, &ctx, thread_idx, smhw);

  ctx.rpc = rpc;
  while (ctx.running) {
    rpc->run_event_loop_once();
  }

  delete rpc;
}

int main(int argc, char **argv) {
  cmdline::parser cmd;
  cmd.add<int>("thread", 't', "thread num", true);
  #if ONESIDE_EXT == 1
  cmd.add<string>("server_ip");
  #endif
  cmd.parse_check(argc, argv);

  #if ONESIDE_EXT == 1
  rdma_rc::RDMAEnv::init();

  rdma_rc::RDMAConnection m_listen_conn;
  m_listen_conn.listen(cmd.get<string>("server_ip"));
  ibv_mr * mr = m_listen_conn.register_memory(1ul << 20);

  printf("--server_port=%u --mr_addr=%lu --mr_rkey=%u\n", m_listen_conn.get_local_addr().second, (uintptr_t)mr->addr, mr->rkey);
  #endif

  auto thread_num = cmd.get<int>("thread");

  std::string server_uri = kServerHostname + ":" + std::to_string(kServerUDPPort);

  erpc::NexusWrap nexus(server_uri);
  if (kMsgSize <= erpc::IBRpcWrap::kMaxDataPerPkt) {
    nexus.register_req_func(kReqType, req_handler);
  } else {
    nexus.register_req_func(kReqType, req_handler2);
  }
  nexus.register_req_func(kSimpleReqType, simple_data_handler);
  nexus.register_req_func(kStopType, stop);

  std::vector<std::thread> threads;
  for (int i = 0; i < thread_num; i++) {
    threads.emplace_back(server_func, &nexus, i);
  }
  for (auto &th : threads) {
    th.join();
  }
}
