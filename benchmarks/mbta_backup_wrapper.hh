#pragma once
#include <atomic>
#include "abstract_db.h"
#include "abstract_ordered_index.h"
#include "sto/Transaction.hh"
#include "sto/LogProto.hh"
#include "sto/MassTrans.hh"

#include "sto/StringWrapper.hh"

#include <unordered_map>
#include <functional>
#include <thread>

#define STD_OP(f) \
  try { \
    f; \
  } catch (Transaction::Abort E) { \
    throw abstract_db::abstract_abort_exception(); \
  }


#if OP_LOGGING
std::atomic<long> mt_get(0);
std::atomic<long> mt_scan(0);
std::atomic<long> mt_rscan(0);
#endif

class mbta_backup_wrapper;

class mbta_backup_ordered_index : public abstract_ordered_index {
public:
  mbta_backup_ordered_index(const std::string &name, uint64_t id, mbta_backup_wrapper *db) : mbta(), name(name), db(db), id(id) {
    Transaction::register_object(mbta, id);
  }

  ~mbta_backup_ordered_index() {
    Transaction::unregister_object(mbta);
  }

  std::string *arena(void);

  bool get(void *txn, lcdf::Str key, std::string &value, size_t max_bytes_read) {
#if OP_LOGGING
    mt_get++;
#endif
    STD_OP({
      bool ret = mbta.transGet(key, value);
      return ret;
    });
  }

  const char *put(void* txn, lcdf::Str key, const std::string &value) {
    NDB_UNIMPLEMENTED("put");
  }

  const char *insert(void *txn, lcdf::Str key, const std::string &value) {
    NDB_UNIMPLEMENTED("insert");
  }

  void remove(void *txn, lcdf::Str key) {
    NDB_UNIMPLEMENTED("remove");
  }

  void scan(void *txn,
      const std::string &start_key,
      const std::string *end_key,
      scan_callback &callback,
      str_arena *arena = nullptr) {
#if OP_LOGGING
    mt_scan++;
#endif
    mbta_type::Str end = end_key ? mbta_type::Str(*end_key) : mbta_type::Str();
    STD_OP(mbta.transQuery(start_key, end, [&] (mbta_type::Str key, std::string& value) {
        return callback.invoke(key.data(), key.length(), value);
    }, arena));
  }

  void rscan(void *txn,
      const std::string &start_key,
      const std::string *end_key,
      scan_callback &callback,
      str_arena *arena = nullptr) {
#if 1
#if OP_LOGGING
    mt_rscan++;
#endif
    mbta_type::Str end = end_key ? mbta_type::Str(*end_key) : mbta_type::Str();
    STD_OP(mbta.transRQuery(start_key, end, [&] (mbta_type::Str key, std::string& value) {
        return callback.invoke(key.data(), key.length(), value);
    }, arena));
#endif
  }

  size_t size() const
  {
    return mbta.approx_size();
  }

  // TODO: unclear if we need to implement, apparently this should clear the tree and possibly return some stats
  std::map<std::string, uint64_t> clear() {
    NDB_UNIMPLEMENTED("clear");
  }

  typedef MassTrans<std::string, versioned_str_struct, false/*opacity*/> mbta_type;

private:
  friend class mbta_backup_wrapper;
  mbta_type mbta;

  const std::string name;
  mbta_backup_wrapper *db;
  uint64_t id;
};

static void thread_init() {
  mbta_backup_ordered_index::mbta_type::thread_init();
}

class mbta_backup_wrapper : public abstract_db {
  std::mutex index_mu;
  std::thread listen_thr;
  TBox<bool> loading_done_box;

public:
  mbta_backup_wrapper(int nthreads, int log_start_port) {
    ALWAYS_ASSERT(log_start_port > 0);

    mbta_backup_ordered_index::mbta_type::static_init();
    pthread_t advancer;
    pthread_create(&advancer, NULL, Transaction::epoch_advancer, NULL);
    pthread_detach(advancer);

    Transaction::register_object(loading_done_box, 0);
    printf("Listening for primary on ports %d-%d\n", log_start_port, log_start_port + nthreads - 1);
    listen_thr = std::thread(LogApply::listen, nthreads, log_start_port,
        std::function<void()>(&::thread_init),
        std::function<void(uint64_t)>(LogApply::default_apply_idle_fn));
  }

  ~mbta_backup_wrapper() {
    listen_thr.join();
  }

  ssize_t txn_max_batch_size() const OVERRIDE { return 100; }

  void do_txn_epoch_sync() const {}

  void do_txn_finish() const {}

  void thread_init(bool loader) {
    mbta_ordered_index::mbta_type::thread_init();

    // spin until loading is done
    while (true) {
      fence();
      if (loading_done_box.nontrans_read())
        break;
      usleep(10);
    }
  }

  void
  thread_end() {}

  size_t
  sizeof_txn_object(uint64_t txn_flags) const
  {
    return sizeof(Transaction);
  }

  static __thread str_arena *thr_arena;
  void *new_txn(
                uint64_t txn_flags,
                str_arena &arena,
                void *buf,
                TxnProfileHint hint = HINT_DEFAULT) {
    // spin until we are idle
    fence();
    while (LogApply::apply_state != LogApply::ApplyState::IDLE) {
      fence();
      usleep(10);
    }
    acquire_fence();

    Sto::start_transaction();
    thr_arena = &arena;
    return NULL;
  }

  bool commit_txn(void *txn) {
    bool ret = Sto::try_commit();
    return ret;
  }

  void abort_txn(void *txn) {
    Sto::silent_abort();
    return;
  }

  abstract_ordered_index *
  open_index(const std::string &name,
             size_t value_size_hint,
	     bool mostly_append = false,
             bool use_hashtable = false) {
    std::unique_lock<std::mutex> lk(index_mu);
    // note that ids start at 1 since we already used 0 for the loading done box
    std::vector<std::string> id_to_name = {
      "customer", "customer_name_idx", "district",
      "history", "item", "new_order",
      "oorder", "oorder_c_id_idx", "order_line",
      "stock", "stock_data", "warehouse"
    };
    for (unsigned i = 0; i < id_to_name.size(); i++) {
      if (name == id_to_name[i])
        return new mbta_backup_ordered_index(name, i + 1, this);
    }
    throw std::string("Index name not found");
  }

 void close_index(abstract_ordered_index *idx) {
   std::unique_lock<std::mutex> lk(index_mu);
   delete idx;
 }

 bool is_read_only_db() {
   return true;
 }
};

__thread str_arena* mbta_backup_wrapper::thr_arena;

std::string *mbta_backup_ordered_index::arena() {
  return (*db->thr_arena)();
}
