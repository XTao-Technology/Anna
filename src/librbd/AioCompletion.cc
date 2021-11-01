// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "common/WorkQueue.h"

#include "librbd/AioObjectRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"

#include "librbd/AioCompletion.h"
#include "librbd/Journal.h"

#ifdef WITH_LTTNG
#include "tracing/librbd.h"
#else
#define tracepoint(...)
#endif

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AioCompletion: "

namespace librbd {

  int AioCompletion::wait_for_complete() {
    tracepoint(librbd, aio_wait_for_complete_enter, this);
    lock.Lock();
    while (state != STATE_COMPLETE)
      cond.Wait(lock);
    lock.Unlock();
    tracepoint(librbd, aio_wait_for_complete_exit, 0);
    return 0;
  }

  void AioCompletion::finalize(ssize_t rval)
  {
    assert(lock.is_locked());
    assert(ictx != nullptr);
    CephContext *cct = ictx->cct;

    ldout(cct, 20) << this << " " << __func__ << ": r=" << rval << ", "
                   << "read_buf=" << reinterpret_cast<void*>(read_buf) << ", "
                   << "real_bl=" <<  reinterpret_cast<void*>(read_bl) << dendl;
    if (rval >= 0 && aio_type == AIO_TYPE_READ) {
      if (read_buf && !read_bl) {
	destriper.assemble_result(cct, read_buf, read_buf_len);
      } else {
	// FIXME: make the destriper write directly into a buffer so
	// that we avoid shuffling pointers and copying zeros around.
	bufferlist bl;
	destriper.assemble_result(cct, bl, true);

	if (read_buf) {
	  assert(bl.length() == read_buf_len);
	  bl.copy(0, read_buf_len, read_buf);
	  ldout(cct, 20) << "copied resulting " << bl.length()
	    << " bytes to " << (void*)read_buf << dendl;
	}
	if (read_bl) {
	  ldout(cct, 20) << " moving resulting " << bl.length()
	    << " bytes to bl " << (void*)read_bl << dendl;
	  read_bl->claim(bl);
	}
      }
    }
  }

  void AioCompletion::complete() {
    assert(lock.is_locked());
    assert(ictx != nullptr);
    CephContext *cct = ictx->cct;

    tracepoint(librbd, aio_complete_enter, this, rval);
    utime_t elapsed;
    elapsed = ceph_clock_now(cct) - start_time;
    switch (aio_type) {
    case AIO_TYPE_OPEN:
    case AIO_TYPE_CLOSE:
      break;
    case AIO_TYPE_READ:
      ictx->perfcounter->tinc(l_librbd_rd_latency, elapsed); break;
    case AIO_TYPE_WRITE:
      ictx->perfcounter->tinc(l_librbd_wr_latency, elapsed); break;
    case AIO_TYPE_DISCARD:
      ictx->perfcounter->tinc(l_librbd_discard_latency, elapsed); break;
    case AIO_TYPE_FLUSH:
      ictx->perfcounter->tinc(l_librbd_aio_flush_latency, elapsed); break;
    default:
      lderr(cct) << "completed invalid aio_type: " << aio_type << dendl;
      break;
    }

    // inform the journal that the op has successfully committed
    if (journal_tid != 0) {
      assert(ictx->journal != NULL);
      ictx->journal->commit_io_event(journal_tid, rval);
    }

    state = STATE_CALLBACK;
    if (complete_cb) {
      lock.Unlock();
      complete_cb(rbd_comp, complete_arg);
      lock.Lock();
    }

    if (ictx && event_notify && ictx->event_socket.is_valid()) {
      ictx->completed_reqs_lock.Lock();
      ictx->completed_reqs.push_back(&m_xlist_item);
      ictx->completed_reqs_lock.Unlock();
      ictx->event_socket.notify();
    }

    state = STATE_COMPLETE;
    cond.Signal();

    // note: possible for image to be closed after op marked finished
    if (async_op.started()) {
      async_op.finish_op();
    }
    tracepoint(librbd, aio_complete_exit);
  }

  void AioCompletion::init_time(ImageCtx *i, aio_type_t t) {
    Mutex::Locker locker(lock);
    if (ictx == nullptr) {
      ictx = i;
      aio_type = t;
      start_time = ceph_clock_now(ictx->cct);
    }
  }

  void AioCompletion::start_op(bool ignore_type) {
    Mutex::Locker locker(lock);
    assert(ictx != nullptr);
    assert(!async_op.started());
    if (state == STATE_PENDING && (ignore_type || aio_type != AIO_TYPE_FLUSH)) {
      async_op.start_op(*ictx);
    }
  }

  void AioCompletion::fail(int r)
  {
    lock.Lock();
    assert(ictx != nullptr);
    CephContext *cct = ictx->cct;

    lderr(cct) << this << " " << __func__ << ": " << cpp_strerror(r)
               << dendl;
    assert(pending_count == 0);
    rval = r;
    complete();
    put_unlock();
  }

  void AioCompletion::set_request_count(uint32_t count) {
    lock.Lock();
    assert(ictx != nullptr);
    CephContext *cct = ictx->cct;

    ldout(cct, 20) << this << " " << __func__ << ": pending=" << count << dendl;
    assert(pending_count == 0);
    pending_count = count;
    lock.Unlock();

    // if no pending requests, completion will fire now
    unblock();
  }

  void AioCompletion::complete_request(ssize_t r)
  {
    lock.Lock();
    assert(ictx != nullptr);
    CephContext *cct = ictx->cct;

    if (rval >= 0) {
      if (r < 0 && r != -EEXIST)
	rval = r;
      else if (r > 0)
	rval += r;
    }
    assert(pending_count);
    int count = --pending_count;

    ldout(cct, 20) << this << " " << __func__ << ": cb=" << complete_cb << ", "
                   << "pending=" << pending_count << dendl;
    if (!count && blockers == 0) {
      finalize(rval);
      complete();
    }
    put_unlock();
  }

  void AioCompletion::associate_journal_event(uint64_t tid) {
    Mutex::Locker l(lock);
    assert(state == STATE_PENDING);
    journal_tid = tid;
  }

  bool AioCompletion::is_complete() {
    tracepoint(librbd, aio_is_complete_enter, this);
    bool done;
    {
      Mutex::Locker l(lock);
      done = this->state == STATE_COMPLETE;
    }
    tracepoint(librbd, aio_is_complete_exit, done);
    return done;
  }

  ssize_t AioCompletion::get_return_value() {
    tracepoint(librbd, aio_get_return_value_enter, this);
    lock.Lock();
    ssize_t r = rval;
    lock.Unlock();
    tracepoint(librbd, aio_get_return_value_exit, r);
    return r;
  }

  void C_AioRead::finish(int r)
  {
    m_completion->lock.Lock();
    CephContext *cct = m_completion->ictx->cct;
    ldout(cct, 10) << "C_AioRead::finish() " << this << " r = " << r << dendl;

    if (r >= 0 || r == -ENOENT) { // this was a sparse_read operation
      ldout(cct, 10) << " got " << m_req->m_ext_map
		     << " for " << m_req->m_buffer_extents
		     << " bl " << m_req->data().length() << dendl;
      // reads from the parent don't populate the m_ext_map and the overlap
      // may not be the full buffer.  compensate here by filling in m_ext_map
      // with the read extent when it is empty.
      if (m_req->m_ext_map.empty())
	m_req->m_ext_map[m_req->m_object_off] = m_req->data().length();

      m_completion->destriper.add_partial_sparse_result(
	  cct, m_req->data(), m_req->m_ext_map, m_req->m_object_off,
	  m_req->m_buffer_extents);
      r = m_req->m_object_len;
    }
    m_completion->lock.Unlock();

    C_AioRequest::finish(r);
  }

  void C_CacheRead::complete(int r) {
    if (!m_enqueued) {
      // cache_lock creates a lock ordering issue -- so re-execute this context
      // outside the cache_lock
      m_enqueued = true;
      m_image_ctx.op_work_queue->queue(this, r);
      return;
    }
    Context::complete(r);
  }

  void C_CacheRead::finish(int r)
  {
    m_req->complete(r);
  }
}
