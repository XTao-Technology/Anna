// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef METAHUNTER_H
#define METAHUNTER_H

#include "common/TrackedOp.h"
#include "common/LogClient.h"
#include "common/Timer.h"

#include "include/types.h"
#include "include/Context.h"
#include "common/Mutex.h"
#include "msg/Dispatcher.h"

#include "MDSMap.h"
#include "SessionMap.h"
#include "MDLog.h"
#include "osdc/Journaler.h"


class MDLog;
struct MutationImpl;
struct MDRequestImpl;
class MClientRequest;
class MClientReply;
class Beacon;
typedef ceph::shared_ptr<MutationImpl> MutationRef;
typedef ceph::shared_ptr<MDRequestImpl> MDRequestRef; 



class C_MDS_RetryJentryRequest : public Context {
public:
  Metahunter *metahunter;
  MDRequestRef mdr;
  C_MDS_RetryJentryRequest(Metahunter *metahunter, MDRequestRef& r);
  void complete(int r);
  void finish(int r);
};


class Metahunter : public Dispatcher
{
public:
  //MDLog *mdlog;
  Mutex mh_lock;
  std::string name;
  MDLog *mdlog;
  // we will mark this flag after mds active start
  MDSRank *mds;
  //bool metahunter_ready;
  //atomic_t initialized;
  OpTracker op_tracker;
  Beacon &beacon;
  bool stopping;
  list<C_MDS_RetryJentryRequest*> jhold_wait_outstanding;
  MDRequestRef outstanding_mdr;
  uint32_t jhold_outstanding_cnt;

  Metahunter(CephContext *cct_, std::string name_, Beacon& beacon_);

  bool ms_dispatch(Message *m);
  void ms_handle_connect(Connection *c) {}
  bool ms_handle_reset(Connection *c) {return false;}
  void ms_handle_remote_reset(Connection *c) {}

  Session *get_session(Message *m);

  void init(MDLog *md_log, MDSRank* mds_rank);
  void handle_journal_init(MDRequestRef& mdr);
  void handle_journal_entrylist_hold(MDRequestRef& mdr);
  void handle_journal_entry_release(MDRequestRef& mdr);
  void reply_mh_request(MDRequestRef& mdr, MClientReply *reply);
  void request_finish(MDRequestRef& mdr);
  void _advance_queues();
  void dispatch_mh_request(MDRequestRef& mdr);
  void _shutdown();
  
  class ProgressThread : public Thread {
    Metahunter *metahunter;
    Cond cond;
  public:
    explicit ProgressThread(Metahunter *metahunter_) : metahunter(metahunter_) {}
    void * entry();
    void shutdown();
    void signal() {cond.Signal();}
  } progress_thread;

  list <Message*> waiting_for_nolaggy;
  list<C_MDS_RetryJentryRequest*> finished_queue;

  void queue_waiters(list<C_MDS_RetryJentryRequest*>& ls) {
    finished_queue.splice(finished_queue.end(), ls);
    progress_thread.signal();
  }

};

#endif
