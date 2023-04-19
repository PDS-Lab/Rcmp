#pragma once

// solym
// ymwh@foxmail.com
// 2020年3月16日 20点28分

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>

template <typename _Kty, typename _Ty>
class SingleFlight {
    // 保存实际执行结果
    struct _Result {
        bool _done;  // 条件量
        // 这里也可以使用读写锁来实现，会简单一点
        // 第一个操作的线程加写锁，后续的线程加读锁，写完成之后，读锁不再阻塞，即可获取结果
        std::mutex _mtx;              // 条件变量互斥锁
        std::condition_variable _cv;  // 条件变量，用于通知
        // _result 用于保存唯一那个真的执行处理的结果
        // 这里需要考虑 Do 参数 func 函数的实际输入输出参数
        // 不一定是返回值
        _Ty _result;
    };
    // 实际执行的结果保存
    struct _Shard {
        std::mutex _domtx;
        std::map<_Kty, std::shared_ptr<_Result>> _do;
    };

    constexpr static size_t _BucketSize = 32;
    _Shard _shards[_BucketSize];

   public:
    // 执行操作
    // key  用于区分请求
    // func 实际执行操作的函数
    // args 实际执行操作函数的参数
    template <class F, typename... Args>
    _Ty Do(_Kty key, F&& func, Args&&... args) {
        size_t shard_index = std::hash<_Kty>{}(key) % _BucketSize;
        auto& shard = _shards[shard_index];
        auto& _domtx = shard._domtx;
        auto& _do = shard._do;

        // 先加写锁，后面可能要修改
        std::unique_lock<std::mutex> lock(_domtx);
        // 判断是否已经存在执行结果
        auto iter = _do.find(key);
        // 存在就等待完成
        if (iter != _do.end()) {
            // 获取实际执行结果结构
            std::shared_ptr<_Result> pRes = iter->second;
            lock.unlock();
            // 等待条件成立(也就是实际执行的那个线程执行完成)
            std::unique_lock<std::mutex> lck(pRes->_mtx);
            pRes->_cv.wait(lck, [pRes]() -> bool { return pRes->_done; });
            // 获取执行结果进行返回
            return pRes->_result;
        }
        // 不存在就创建一个操作结果
        std::shared_ptr<_Result> pRes = std::make_shared<_Result>();
        pRes->_done = false;  // 设置初始条件为 false
        _do[key] = pRes;
        lock.unlock();  // 解锁，别的线程能够继续

        // 2020年8月27日 更新
        // 在网上看到这篇文章  缓存击穿导致 golang 组件死锁的问题分享
        //                   https://mp.weixin.qq.com/s/XsCTQJWry2UUtAkM4OFbwA
        // 文章里面说得很清楚了，如果下面执行 func 的时候，产生了异常，在这个函数外面
        // catch 处理了，则程序不会退出，其它等待者也不会被唤醒，后面的 _do.erase(key)
        // 也不会执行，导致现有的等待者一直等待，后续来的也会因为 key 存在而进入等待
        // 执行真正的操作，获取返回结果
        pRes->_result = func(args...);
        {
            std::lock_guard<std::mutex> lck(pRes->_mtx);
            pRes->_done = true;
        }
        // 通知所有线程
        pRes->_cv.notify_all();
        // 移除（也可以放在一个延迟移除队列，进行异步移除，以便后续的
        // 相同key操作也可以直接使用。但在这外面缓存结果会更好）
        lock.lock();
        _do.erase(key);
        return pRes->_result;
    }
};