/*
 Copyright (C) 2021 XTAO Technology <peng.hse@xtaotech.com>.
*/

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "common/dout.h"
#include "include/stringify.h"
#include "include/util.h"

#include "messages/MClientSession.h"
#include "messages/MClientRequest.h"
#include "messages/MClientReply.h"
#include "Mutation.h"
#include "MDLog.h"
#include "Metahunter.h"
#include "MDSRank.h"

#include <errno.h>
#include <iostream>

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds.metahunter."


C_MDS_RetryJentryRequest::C_MDS_RetryJentryRequest(Metahunter *metahunter_,
						   MDRequestRef& r) :
  metahunter(metahunter_), mdr(r)
{}

void C_MDS_RetryJentryRequest::complete(int r)
{
  assert(NULL != metahunter);
  assert(metahunter->mh_lock.is_locked_by_me());
  dout(10) << "C_MDS_RetryJentryRequest::complete" << dendl;
  Context::complete(r);
}

void C_MDS_RetryJentryRequest::finish(int r)
{
  mdr->retry++;
  //mlog->dispatch_request(mdr);
  metahunter->dispatch_mh_request(mdr);
}

Metahunter::Metahunter(CephContext *cct_, std::string name_, Beacon& beacon_) :
  Dispatcher(cct_),
  mh_lock("Metahunter"),
  name(name_),
  op_tracker(g_ceph_context, g_conf->mds_enable_mh_tracker, 1),
  beacon(beacon_),
  stopping(false),
  outstanding_mdr(NULL),
  jhold_outstanding_cnt(0),
  progress_thread(this)
{
  op_tracker.set_complaint_and_threshold(g_conf->mds_op_complaint_time,
                                         g_conf->mds_op_log_threshold);
  op_tracker.set_history_size_and_duration(g_conf->mds_op_history_size,
                                           g_conf->mds_op_history_duration);
}

							  							  
void *Metahunter::ProgressThread::entry()
{
  Mutex::Locker l(metahunter->mh_lock);
  while (true) {
    while (metahunter->finished_queue.empty()) {
      cond.Wait(metahunter->mh_lock);
    }
    if (metahunter->stopping) {
      break;
    }
   metahunter-> _advance_queues();
  }
  return NULL;
}


void Metahunter::ProgressThread::shutdown()
{
  assert(metahunter->mh_lock.is_locked_by_me());
  assert(metahunter->stopping);

  if (am_self()) {
    // fall through
  } else {
    cond.Signal();
    metahunter->mh_lock.Unlock();
    if (is_started())
      join();
    metahunter->mh_lock.Lock();
  }
}


void Metahunter::_shutdown()
{
  stopping = true;
  op_tracker.on_shutdown();
  progress_thread.shutdown();
}

void Metahunter::_advance_queues()
{
  assert(mh_lock.is_locked_by_me());
  while (!finished_queue.empty()) {
    dout(7) << "metahunter has " << finished_queue.size() <<
      " queued contexts " << dendl;
    dout(10) << finished_queue << dendl;
    list<C_MDS_RetryJentryRequest*> ls;
    ls.swap(finished_queue);
    while (!ls.empty()) {
      dout(10) << " finished " << ls.front() << dendl;
      ls.front()->complete(0);
      ls.pop_front();
    }
  }

}


void  Metahunter::init(MDLog *md_log, MDSRank* mds_rank)
{
  //initialized.set(1);
  mdlog = md_log;
  mds = mds_rank;
  mdlog->set_metahunter(this);
  progress_thread.create("metahunter");
}


Session *Metahunter::get_session(Message *m)
{
  Session *session = static_cast<Session *>(m->get_connection()->get_priv());
  if (session) {
    dout(20) << "get_session have " << session << " " << session->info.inst
	     << " state " << session->get_state_name() << dendl;
    session->put();  // not carry ref
  } else {
    dout(20) << "get_session dne for " << m->get_source_inst() << dendl;
  }
  return session;
}


void Metahunter::handle_journal_init(MDRequestRef& mdr)
{
  dout(1) << "enter journal init " << dendl;
  if (!mdlog->allow_mh()) {
    reply_mh_request(mdr, new MClientReply(mdr->client_request, -5));
    return;
  }
  if (mdlog->ad_journal_init()) {
    mdlog->jentry_queue_wait(mdr, this);
    return;
  }
  reply_mh_request(mdr, new MClientReply(mdr->client_request, 0));
}

void Metahunter::handle_journal_entrylist_hold(MDRequestRef& mdr)
{
  vector<journal_entry> jentryvec;
  map<uint64_t, journal_entry> mergeattr_map;
  bufferlist jentrybl;
  uint32_t num_jentry = 0;
  uint32_t max_jentry_num = g_conf->mds_journal_max_prefetch;

  //TODO: take a deep thought on the mds_lock here
  //mds->mds_lock.Unlock();
  journal_entry *debug_pjentry = NULL;
  dout(20) << "enter hold journal entrylist" << dendl;
  
  //Peng happynewyear
  if (jhold_outstanding_cnt && mdr.get() != outstanding_mdr.get()) {
    dout(1) << "has jhold_outstanding_cnt " << jhold_outstanding_cnt << " must wait "
	    << dendl;
    C_MDS_RetryJentryRequest *c_hold = new C_MDS_RetryJentryRequest(this, mdr);
    jhold_wait_outstanding.push_back(c_hold);
    return;
  } else if (jhold_outstanding_cnt && (mdr.get() == outstanding_mdr.get())) {
    // myself retry, let it pass
  } else {
    jhold_outstanding_cnt++;
    assert(NULL == outstanding_mdr);
    outstanding_mdr = mdr;
  }

  while(num_jentry < max_jentry_num) {
    journal_entry jentry;
    mdlog->ad_read_journal_entry(jentry, mdr, this);
    if ((op_done == jentry.op) && (0 == num_jentry)) {
      // got nothing wait till new journal entry comes in
      mdlog->jentry_queue_wait(mdr, this);
      return;
    } else if (op_done == jentry.op) {
      // meet the end of journal but 
      // have already packed serveral jentries, reply to client what we got.
      // not forget to rollback the ad read pos in order to match the
      // release pos update
      dout(1) << " rollback the ad read pos, mdr->jlen " << mdr->jlen << dendl;
      //mdlog->dec_ad_read_pos(mdr->jlen);
      journal_entry prev_jentry = jentryvec.back();
      prev_jentry.len += mdr->jlen;
      jentryvec.pop_back();
      jentryvec.push_back(prev_jentry);
      if ((op_setattr == prev_jentry.op) &&
	  mergeattr_map.count(prev_jentry.fid.val)) {
	mergeattr_map[prev_jentry.fid.val].len = prev_jentry.len;
      }
      break;
    }
    
    //peng2debug
    dout(20) << "peng-debug num_jentry " << num_jentry <<
      " jentry op is " << jentry.op <<
      " name " << jentry.name <<
      " parentid " << jentry.parentid <<
      " fid " << jentry.fid <<
      " seq " << jentry.seq <<
      " len " << jentry.len << dendl;

    if (!jentryvec.empty()) {
      debug_pjentry = &jentryvec.back();
      assert((debug_pjentry->seq + debug_pjentry->len) == jentry.seq );
    }

    jentryvec.push_back(jentry);
    num_jentry++;
    if (op_setattr == jentry.op) {
      if (mergeattr_map.count(jentry.fid.val)) {
	mergeattr_map[jentry.fid.val].swap(jentry);
	num_jentry--;
      } else {
	mergeattr_map.insert(make_pair(jentry.fid.val, jentry));
      }
    }
    mdr->jlen = 0;
    
  }
  
  /* mergeattr map contains the jentries having the same unique fid for
   * the "setattr" op. while the jentryvec  contains all the jentries visited
   * during the "hold" round. we merage the multiple setattr op jentries
   * into the last one jentry */

  ::encode(num_jentry, jentrybl);
  uint32_t idx = 0;
  debug_pjentry = NULL;
  uint32_t debug_count = 0;
  for (vector<journal_entry>::iterator it = jentryvec.begin();
       it != jentryvec.end();
       ++it, ++idx) {
    dout(20) << "jentryvec[ " << idx << "].op " << (*it).op << " seq " <<
      (*it).seq << " fid " << (*it).fid.val << dendl;
    
    if ((op_setattr == (*it).op) &&
	mergeattr_map.count((*it).fid.val) &&
	((*it).seq < mergeattr_map[(*it).fid.val].seq)) {
      dout(20) << "op_setattr jentryvec[ " << idx << "].seq " << (*it).seq << " < "
	       << "mergeattr_map[ " << (*it).fid.val << "].seq " << mergeattr_map[(*it).fid.val].seq 
	       << " for fid " << (*it).fid.val << dendl;
      jentryvec.at(idx+1);
      assert(jentryvec[idx].seq == (*it).seq);
      dout(20) << "jentryvec[ " << idx << "+1].seq " <<  jentryvec[idx+1].seq <<
	" len " <<  jentryvec[idx+1].len << dendl;
      
      if ((op_setattr == jentryvec[idx+1].op) &&
	  mergeattr_map.count(jentryvec[idx+1].fid.val) &&
	  (mergeattr_map[jentryvec[idx+1].fid.val].seq == jentryvec[idx+1].seq)) {
	dout(20) << "adjust mergeattr_map[" << jentryvec[idx+1].fid.val << "].seq from " <<
	  mergeattr_map[jentryvec[idx+1].fid.val].seq << " to " << jentryvec[idx+1].seq << dendl;
	
	mergeattr_map[jentryvec[idx+1].fid.val].seq = jentryvec[idx].seq;
	mergeattr_map[jentryvec[idx+1].fid.val].len += jentryvec[idx].len;
      }
      
      jentryvec[idx+1].seq = jentryvec[idx].seq;
      jentryvec[idx+1].len += jentryvec[idx].len;
      
      dout(20) << "merge jentryvec[" << idx << "].seq " <<  jentryvec[idx].seq <<
	" with jentryvec[" << idx+1 << "].seq " << jentryvec[idx+1].seq
	       << "fid is " << jentryvec[idx+1].fid.val << dendl;
      
      continue;
    } 
    
    if ((op_setattr == (*it).op) && mergeattr_map.count((*it).fid.val)) {
      dout(20) << "mergeattr_map[ " << (*it).fid.val <<
	"].seq= " << mergeattr_map[(*it).fid.val].seq << " (*it).seq= " <<
	(*it).seq << " num_jentry " << num_jentry << " idx " << idx << dendl;
      assert((*it).seq == mergeattr_map[(*it).fid.val].seq);
      if (debug_pjentry) {
	assert((debug_pjentry->seq + debug_pjentry->len) == mergeattr_map[(*it).fid.val].seq);
      }
      dout(20) << "willencode mergeattr_map jentry seq " << mergeattr_map[(*it).fid.val].seq <<
	" len " << mergeattr_map[(*it).fid.val].len << " op " << mergeattr_map[(*it).fid.val].op << dendl;
      mergeattr_map[(*it).fid.val].encode(jentrybl);
      debug_count++;
      debug_pjentry = &(mergeattr_map[(*it).fid.val]);
      continue;
    }
    if (debug_pjentry) {
      assert((debug_pjentry->seq + debug_pjentry->len) == (*it).seq);
    }
    dout(20) << "willencode jentry seq " << (*it).seq <<
      " len " << (*it).len << " op " << (*it).op << dendl;
    (*it).encode(jentrybl);
    debug_count++;
    debug_pjentry = &(*it);
  }
  assert(debug_count == num_jentry);
  mdr->reply_extra_bl = jentrybl;
  // Peng happynewyear
  assert((1 == jhold_outstanding_cnt) && (outstanding_mdr.get() == mdr.get()));
  //TODO could save the use of jhold_outstanding_cnt
  jhold_outstanding_cnt--;
  // make sure the reset will free the mdr mem
  outstanding_mdr.reset();
  if (!jhold_wait_outstanding.empty()) {
    list<C_MDS_RetryJentryRequest*> finished;
    finished.swap(jhold_wait_outstanding);
    dout(1) << " trigger jhold_wait_ourstanding mdr " << finished.back() << dendl;
    queue_waiters(finished);
  }
  
  reply_mh_request(mdr,
		   new MClientReply(mdr->client_request, 0, CEPH_MSG_MH_REPLY));
}


void Metahunter::handle_journal_entry_release(MDRequestRef& mdr)
{
  MClientRequest *req = mdr->client_request;
  uint64_t seq = req->head.args.mh.seq;
  uint64_t len = req->head.args.mh.len;
  dout(20) << "enter journal entry release seq " << seq <<
    " length " << len << dendl;
  if (0 == len) {
    dout(1) << "handle_journal_entry_release len is 0, seq " << seq << dendl;
    assert(0 != len);  
  }
  
  //mdlog->ad_release_journal_entry(seq);
  mdlog->ad_release_journal_entry(seq, len);
  
  reply_mh_request(mdr,
		   new MClientReply(mdr->client_request, 0, CEPH_MSG_MH_REPLY));
}


void Metahunter::reply_mh_request(MDRequestRef& mdr, MClientReply *reply)
{
  assert(mdr.get());
  MClientRequest *req = mdr->client_request;
  
  dout(10) << "reply_mh_request " << reply->get_result() 
	   << " (" << cpp_strerror(reply->get_result())
	   << ") " << *req << dendl;
  
  mdr->mark_event("replying");
  
  
  //bool is_replay = mdr->client_request->is_replay();
  //bool did_early_reply = mdr->did_early_reply;


  
  //if (!did_early_reply && !is_replay) {
    
  //  mds->logger->inc(l_mds_reply);
  //  utime_t lat = ceph_clock_now(g_ceph_context) - mdr->client_request->get_recv_stamp();
  //mds->logger->tinc(l_mds_reply_latency, lat);
  // dout(20) << "lat " << lat << dendl;
  //}
  
  // note client connection to direct my reply
  ConnectionRef client_con = req->get_connection();
  
  // drop non-rdlocks before replying, so that we can issue leases
  //mdcache->request_drop_non_rdlocks(mdr);
  

  // We can set the extra bl unconditionally: if it's already been sent in the
  // early_reply, set_extra_bl will have claimed it and reply_extra_bl is empty
  reply->set_extra_bl(mdr->reply_extra_bl);
  
  reply->set_mdsmap_epoch(mds->mdsmap->get_epoch());
  client_con->send_message(reply);
  
  // clean up request
  request_finish(mdr);
}

void Metahunter::request_finish(MDRequestRef& mdr)
{
  mdr->item_session_request.remove_myself();
  mdr->mark_event("cleaned up request");
}

void Metahunter::dispatch_mh_request(MDRequestRef& mdr)
{
  MClientRequest *req = mdr->client_request;
  int op = req->get_op();
  Session *session = 0;
  bool drop = false;
  // NOTE: since i do hold mh_lock instead of mds_lock to get the session
  // is it possible that the session state is in a risk condition
  session = get_session(req);
  ConnectionRef& pipe_con = session->connection;

  if (!session) {
    dout(1) << "no mh session for " << req->get_source() << ", dropping op" <<
      op << dendl;
    //request_finish(mdr);
    //return;
    drop = true;
    goto out;
  }
  if (session->is_closed() ||
      session->is_closing() ||
      session->is_killing()) {
    dout(1) << "mh session closed|closing|killing, dropping op " << op << dendl;
    //request_finish(mdr);
    //return;
    drop = true;
    goto out;
  }
  
  if (!pipe_con->is_connected()) {
    dout(1) << "peng mh session connection is not connected, dropping op " <<
	    op << dendl;
    drop = true;
    //request_finish(mdr);
    //return;
    goto out;
  }
  
  dout(7) << " dispatch_mh_request " << *req << dendl;

  switch(req->get_op()) {
  case CEPH_MDS_OP_JOURNAL_INIT:
    handle_journal_init(mdr);
    break;
  case CEPH_MDS_OP_JOURNAL_ENTRYLIST_HOLD:
    handle_journal_entrylist_hold(mdr);
    break;
  case CEPH_MDS_OP_JOURNAL_ENTRY_RELEASE:
    handle_journal_entry_release(mdr);
    break;
  default:
    assert(0);
  }

out:
  if (drop) {
    if (jhold_outstanding_cnt && (outstanding_mdr.get() == mdr.get())) {
      assert(1 == jhold_outstanding_cnt);
      jhold_outstanding_cnt--;
      // make sure the reset will free the mdr mem
      outstanding_mdr.reset();
      if (!jhold_wait_outstanding.empty()) {
	list<C_MDS_RetryJentryRequest*> finished;
	finished.swap(jhold_wait_outstanding);
	dout(1) << " trigger jhold_wait_ourstanding mdr " << finished.back() << dendl;
	queue_waiters(finished);
      }
    }
    request_finish(mdr);
  }

  return;
}

bool Metahunter::ms_dispatch(Message *m)
{
  Mutex::Locker l(mh_lock);
  MClientRequest *req = static_cast<MClientRequest*>(m);
  int op = req->get_op();
  if (CEPH_MDS_OP_JOURNAL_INIT == op) {
    ldout(cct, 1) << "metahunter ms_dispatch journal init " << dendl;
  }
  if (CEPH_MSG_CLIENT_REQUEST != m->get_type()) {
    return false;
  }
  ldout(cct, 10) << __func__ << " " << cct << " " << *m << dendl;
  //if(!initialized.read())
  //  return false;
  // if mds was not active yet, we should find a way to wait till it active
  if (beacon.get_want_state() == CEPH_MDS_STATE_DNE) {
    dout(1) << " metahunter stopping, discarding " << *m << " op is " <<
	    op << dendl;
    m->put();
    return true;
  }

  
  
  if (!((CEPH_MDS_OP_JOURNAL_INIT == op) ||
	(CEPH_MDS_OP_JOURNAL_ENTRYLIST_HOLD == op) ||
	(CEPH_MDS_OP_JOURNAL_ENTRY_RELEASE == op))) {
    return false;
  }
  
  MDRequestImpl::Params params;
  params.reqid = req->get_reqid();
  params.attempt = req->get_num_fwd();
  params.client_req = req;
  params.initiated = req->get_recv_stamp();
  params.throttled = req->get_throttle_stamp();
  params.all_read = req->get_recv_complete_stamp();
  params.dispatched = req->get_dispatch_stamp();
  
  MDRequestRef mdr =
    op_tracker.create_request<MDRequestImpl,MDRequestImpl::Params>(params);

  Session *session = 0;
  session = get_session(req);
  mdr->session = session;
  session->requests.push_back(&mdr->item_session_request);

  // first try previous jentry hold waiting request
  _advance_queues();
  // then do the current request
  // note that the concurrent retry req and undergoing req will sequentially read
  // the journal
  dispatch_mh_request(mdr);
  
  return true;
}
