// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <unordered_map>

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "gen_cpp/InternalService_types.h" // for TQueryOptions
#include "gen_cpp/Types_types.h"           // for TUniqueId
#include "runtime/runtime_state.h"
#include "util/hash_util.hpp"
#include "util/time.h"

namespace starrocks {
namespace pipeline {

using std::chrono::seconds;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::duration_cast;
// The context for all fragment of one query in one BE
class QueryContext {
public:
    QueryContext();
    ~QueryContext();
    void set_exec_env(ExecEnv* exec_env) { _exec_env = exec_env; }
    void set_query_id(const TUniqueId& query_id) { _query_id = query_id; }
    TUniqueId query_id() { return _query_id; }
    void set_total_fragments(size_t total_fragments) { _total_fragments = total_fragments; }

    void increment_num_fragments() {
        _num_fragments.fetch_add(1);
        _num_active_fragments.fetch_add(1);
    }

    bool count_down_fragments() { return _num_active_fragments.fetch_sub(1) == 1; }

    bool is_finished() { return _num_active_fragments.load() == 0; }

    void set_expire_seconds(int expire_seconds) { _expire_seconds = seconds(expire_seconds); }

    // now time point pass by deadline point.
    bool is_expired() {
        auto now = duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
        return is_finished() && now > _deadline;
    }

    bool is_dead() { return _num_active_fragments == 0 && _num_fragments == _total_fragments; }
    // add expired seconds to deadline
    void extend_lifetime() {
        _deadline = duration_cast<milliseconds>(steady_clock::now().time_since_epoch() + _expire_seconds).count();
    }

    FragmentContextManager* fragment_mgr();

    void cancel(const Status& status);

    void set_is_runtime_filter_coordinator(bool flag) { _is_runtime_filter_coordinator = flag; }

    ObjectPool* object_pool() { return &_object_pool; }
    void set_desc_tbl(DescriptorTbl* desc_tbl) {
        DCHECK(_desc_tbl == nullptr);
        _desc_tbl = desc_tbl;
    }

    DescriptorTbl* desc_tbl() {
        DCHECK(_desc_tbl != nullptr);
        return _desc_tbl;
    }

    void init_mem_tracker(MemTracker* parent, int64_t limit);
    MemTracker* mem_tracker() { return _mem_tracker.get(); }

    void incr_cpu_cost(int64_t cost) { _cur_cpu_cost += cost; }
    int64_t cpu_cost() const { return _cur_cpu_cost; }

    // Record the number of rows read from the data source for big query checking
    void incr_cur_scan_rows_num(int64_t rows_num) { _cur_scan_rows_num += rows_num; }
    int64_t cur_scan_rows_num() const { return _cur_scan_rows_num; }

    // Record the cpu time of the query run, for big query checking
    int64_t init_wg_cpu_cost() const { return _init_wg_cpu_cost; }
    void set_init_wg_cpu_cost(int64_t wg_cpu_cost) { _init_wg_cpu_cost = wg_cpu_cost; }

    // Query start time, used to check how long the query has been running
    // To ensure that the minimum run time of the query will not be killed by the big query checking mechanism
    int64_t query_begin_time() const { return _query_begin_time; }
    void init_query_begin_time() { _query_begin_time = MonotonicNanos(); }

private:
    ExecEnv* _exec_env = nullptr;
    TUniqueId _query_id;
    std::unique_ptr<FragmentContextManager> _fragment_mgr;
    size_t _total_fragments;
    std::atomic<size_t> _num_fragments;
    std::atomic<size_t> _num_active_fragments;
    int64_t _deadline;
    seconds _expire_seconds;
    bool _is_runtime_filter_coordinator = false;
    ObjectPool _object_pool;
    DescriptorTbl* _desc_tbl = nullptr;

    std::shared_ptr<starrocks::MemTracker> _mem_tracker = nullptr;

    int64_t _query_begin_time = 0;
    std::atomic<int64_t> _cur_cpu_cost = 0;
    std::atomic<int64_t> _cur_scan_rows_num = 0;

    int64_t _init_wg_cpu_cost = 0;
};

class QueryContextManager {
    DECLARE_SINGLETON(QueryContextManager);

public:
#ifdef BE_TEST
    explicit QueryContextManager(int);
#endif
    QueryContext* get_or_register(const TUniqueId& query_id);
    QueryContextPtr get(const TUniqueId& query_id);
    void remove(const TUniqueId& query_id);
    // used for graceful exit
    void clear();

private:
    std::vector<std::shared_mutex> _mutexes;
    std::vector<std::unordered_map<TUniqueId, QueryContextPtr>> _context_maps;
    std::vector<std::unordered_map<TUniqueId, QueryContextPtr>> _second_chance_maps;
};

} // namespace pipeline
} // namespace starrocks
