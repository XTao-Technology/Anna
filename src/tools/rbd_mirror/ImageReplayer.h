// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
#define CEPH_RBD_MIRROR_IMAGE_REPLAYER_H

#include <map>
#include <string>
#include <vector>

#include "include/atomic.h"
#include "common/Mutex.h"
#include "common/WorkQueue.h"
#include "include/rados/librados.hpp"
#include "cls/journal/cls_journal_types.h"
#include "cls/rbd/cls_rbd_types.h"
#include "journal/ReplayEntry.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "ProgressContext.h"
#include "types.h"
#include <boost/optional.hpp>

class AdminSocketHook;

namespace journal {

class Journaler;
class ReplayHandler;

}

namespace librbd {

class ImageCtx;
namespace journal { template <typename> class Replay; }

}

namespace rbd {
namespace mirror {

struct Threads;

namespace image_replayer { template <typename> class BootstrapRequest; }
namespace image_replayer { template <typename> class ReplayStatusFormatter; }

/**
 * Replays changes from a remote cluster for a single image.
 */
template <typename ImageCtxT = librbd::ImageCtx>
class ImageReplayer {
public:
  typedef typename librbd::journal::TypeTraits<ImageCtxT>::ReplayEntry ReplayEntry;

  enum State {
    STATE_UNKNOWN,
    STATE_STARTING,
    STATE_REPLAYING,
    STATE_STOPPING,
    STATE_STOPPED,
  };

  struct BootstrapParams {
    std::string local_image_name;

    BootstrapParams() {}
    BootstrapParams(const std::string local_image_name) :
      local_image_name(local_image_name) {}

    bool empty() const {
      return local_image_name.empty();
    }
  };

  ImageReplayer(Threads *threads, RadosRef local, RadosRef remote,
		const std::string &local_mirror_uuid,
                const std::string &remote_mirror_uuid, int64_t local_pool_id,
		int64_t remote_pool_id, const std::string &remote_image_id,
                const std::string &global_image_id);
  virtual ~ImageReplayer();
  ImageReplayer(const ImageReplayer&) = delete;
  ImageReplayer& operator=(const ImageReplayer&) = delete;

  State get_state() { Mutex::Locker l(m_lock); return get_state_(); }
  bool is_stopped() { Mutex::Locker l(m_lock); return is_stopped_(); }
  bool is_running() { Mutex::Locker l(m_lock); return is_running_(); }

  std::string get_name() { Mutex::Locker l(m_lock); return m_name; };
  void set_state_description(int r, const std::string &desc);

  inline int64_t get_local_pool_id() const {
    return m_local_pool_id;
  }
  inline int64_t get_remote_pool_id() const {
    return m_remote_pool_id;
  }
  inline const std::string& get_global_image_id() const {
    return m_global_image_id;
  }
  inline const std::string& get_remote_image_id() const {
    return m_remote_image_id;
  }
  inline std::string get_local_image_id() {
    Mutex::Locker locker(m_lock);
    return m_local_image_id;
  }
  inline std::string get_local_image_name() {
    Mutex::Locker locker(m_lock);
    return m_local_image_name;
  }

  void start(Context *on_finish = nullptr,
	     const BootstrapParams *bootstrap_params = nullptr,
	     bool manual = false);
  void stop(Context *on_finish = nullptr, bool manual = false);
  void restart(Context *on_finish = nullptr);
  void flush(Context *on_finish = nullptr);

  void print_status(Formatter *f, stringstream *ss);

  virtual void handle_replay_ready();
  virtual void handle_replay_complete(int r, const std::string &error_desc);

protected:
  /**
   * @verbatim
   *                   (error)
   * <uninitialized> <------------------------------------ FAIL
   *    |                                                   ^
   *    v                                                   *
   * <starting>                                             *
   *    |                                                   *
   *    v                                           (error) *
   * BOOTSTRAP_IMAGE  * * * * * * * * * * * * * * * * * * * *
   *    |                                                   *
   *    v                                           (error) *
   * INIT_REMOTE_JOURNALER  * * * * * * * * * * * * * * * * *
   *    |                                                   *
   *    v                                           (error) *
   * START_REPLAY * * * * * * * * * * * * * * * * * * * * * *
   *    |
   *    |  /--------------------------------------------\
   *    |  |                                            |
   *    v  v   (asok flush)                             |
   * REPLAYING -------------> LOCAL_REPLAY_FLUSH        |
   *    |       \                 |                     |
   *    |       |                 v                     |
   *    |       |             FLUSH_COMMIT_POSITION     |
   *    |       |                 |                     |
   *    |       |                 \--------------------/|
   *    |       |                                       |
   *    |       | (entries available)                   |
   *    |       \-----------> REPLAY_READY              |
   *    |                         |                     |
   *    |                         | (skip if not        |
   *    |                         v  needed)        (error)
   *    |                     REPLAY_FLUSH  * * * * * * * * *
   *    |                         |                     |   *
   *    |                         | (skip if not        |   *
   *    |                         v  needed)        (error) *
   *    |                     GET_REMOTE_TAG  * * * * * * * *
   *    |                         |                     |   *
   *    |                         | (skip if not        |   *
   *    |                         v  needed)        (error) *
   *    |                     ALLOCATE_LOCAL_TAG  * * * * * *
   *    |                         |                     |   *
   *    |                         v                 (error) *
   *    |                     PROCESS_ENTRY * * * * * * * * *
   *    |                         |                     |   *
   *    |                         \---------------------/   *
   *    v                                                   *
   * REPLAY_COMPLETE  < * * * * * * * * * * * * * * * * * * *
   *    |
   *    v
   * JOURNAL_REPLAY_SHUT_DOWN
   *    |
   *    v
   * LOCAL_IMAGE_CLOSE
   *    |
   *    v
   * <stopped>
   *
   * @endverbatim
   */

  virtual void on_start_fail(int r, const std::string &desc = "");
  virtual bool on_start_interrupted();

  virtual void on_stop_journal_replay();

  virtual void on_flush_local_replay_flush_start(Context *on_flush);
  virtual void on_flush_local_replay_flush_finish(Context *on_flush, int r);
  virtual void on_flush_flush_commit_position_start(Context *on_flush);
  virtual void on_flush_flush_commit_position_finish(Context *on_flush, int r);

  bool on_replay_interrupted();

  void close_local_image(Context *on_finish); // for tests

private:
  typedef typename librbd::journal::TypeTraits<ImageCtxT>::Journaler Journaler;
  typedef boost::optional<State> OptionalState;

  class BootstrapProgressContext : public ProgressContext {
  public:
    BootstrapProgressContext(ImageReplayer<ImageCtxT> *replayer) :
      replayer(replayer) {
    }

    virtual void update_progress(const std::string &description,
				 bool flush = true);
  private:
    ImageReplayer<ImageCtxT> *replayer;
  };

  Threads *m_threads;
  RadosRef m_local, m_remote;
  std::string m_local_mirror_uuid;
  std::string m_remote_mirror_uuid;
  int64_t m_remote_pool_id, m_local_pool_id;
  std::string m_remote_image_id, m_local_image_id, m_global_image_id;
  std::string m_local_image_name;
  std::string m_name;
  Mutex m_lock;
  State m_state = STATE_STOPPED;
  int m_last_r = 0;
  std::string m_state_desc;
  BootstrapProgressContext m_progress_cxt;
  image_replayer::ReplayStatusFormatter<ImageCtxT> *m_replay_status_formatter =
    nullptr;
  librados::IoCtx m_local_ioctx, m_remote_ioctx;
  ImageCtxT *m_local_image_ctx = nullptr;
  librbd::journal::Replay<ImageCtxT> *m_local_replay = nullptr;
  Journaler* m_remote_journaler = nullptr;
  ::journal::ReplayHandler *m_replay_handler = nullptr;

  Context *m_on_start_finish = nullptr;
  Context *m_on_stop_finish = nullptr;
  Context *m_update_status_task = nullptr;
  int m_update_status_interval = 0;
  librados::AioCompletion *m_update_status_comp = nullptr;
  bool m_stop_requested = false;
  bool m_manual_stop = false;

  AdminSocketHook *m_asok_hook = nullptr;

  image_replayer::BootstrapRequest<ImageCtxT> *m_bootstrap_request = nullptr;

  uint32_t m_in_flight_status_updates = 0;
  bool m_update_status_requested = false;
  Context *m_on_update_status_finish = nullptr;

  librbd::journal::MirrorPeerClientMeta m_client_meta;

  ReplayEntry m_replay_entry;
  bool m_replay_tag_valid = false;
  uint64_t m_replay_tag_tid = 0;
  cls::journal::Tag m_replay_tag;
  librbd::journal::TagData m_replay_tag_data;

  struct C_ReplayCommitted : public Context {
    ImageReplayer *replayer;
    ReplayEntry replay_entry;

    C_ReplayCommitted(ImageReplayer *replayer,
                      ReplayEntry &&replay_entry)
      : replayer(replayer), replay_entry(std::move(replay_entry)) {
    }
    virtual void finish(int r) {
      replayer->handle_process_entry_safe(replay_entry, r);
    }
  };

  static std::string to_string(const State state);

  State get_state_() const {
    return m_state;
  }
  bool is_stopped_() const {
    return m_state == STATE_STOPPED;
  }
  bool is_running_() const {
    return !is_stopped_() && m_state != STATE_STOPPING && !m_stop_requested;
  }

  bool update_mirror_image_status(bool force, const OptionalState &state);
  bool start_mirror_image_status_update(bool force, bool restarting);
  void finish_mirror_image_status_update();
  void queue_mirror_image_status_update(const OptionalState &state);
  void send_mirror_status_update(const OptionalState &state);
  void handle_mirror_status_update(int r);
  void reschedule_update_status_task(int new_interval = 0);

  void shut_down(int r, Context *on_start);
  void handle_shut_down(int r, Context *on_start);

  void bootstrap();
  void handle_bootstrap(int r);

  void init_remote_journaler();
  void handle_init_remote_journaler(int r);

  void start_replay();
  void handle_start_replay(int r);

  void replay_flush();
  void handle_replay_flush(int r);

  void get_remote_tag();
  void handle_get_remote_tag(int r);

  void allocate_local_tag();
  void handle_allocate_local_tag(int r);

  void process_entry();
  void handle_process_entry_ready(int r);
  void handle_process_entry_safe(const ReplayEntry& replay_entry, int r);

};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::ImageReplayer<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_IMAGE_REPLAYER_H
