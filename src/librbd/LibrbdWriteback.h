// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_LIBRBDWRITEBACKHANDLER_H
#define CEPH_LIBRBD_LIBRBDWRITEBACKHANDLER_H

#include <queue>

#include "include/Context.h"
#include "include/types.h"
#include "include/rados/librados.hpp"
#include "osd/osd_types.h"
#include "osdc/WritebackHandler.h"

class Mutex;

namespace librbd {

  struct ImageCtx;

  class LibrbdWriteback : public WritebackHandler {
  public:
    LibrbdWriteback(ImageCtx *ictx, Mutex& lock);

    // Note that oloc, trunc_size, and trunc_seq are ignored
    virtual void read(const object_t& oid, uint64_t object_no,
		      const object_locator_t& oloc, uint64_t off, uint64_t len,
		      snapid_t snapid, bufferlist *pbl, uint64_t trunc_size,
		      __u32 trunc_seq, int op_flags, Context *onfinish);

    // Determine whether a read to this extent could be affected by a
    // write-triggered copy-on-write
    virtual bool may_copy_on_write(const object_t& oid, uint64_t read_off,
				   uint64_t read_len, snapid_t snapid);

    // Note that oloc, trunc_size, and trunc_seq are ignored
    virtual ceph_tid_t write(const object_t& oid, const object_locator_t& oloc,
			     uint64_t off, uint64_t len,
			     const SnapContext& snapc, const bufferlist &bl,
			     ceph::real_time mtime, uint64_t trunc_size,
			     __u32 trunc_seq, ceph_tid_t journal_tid,
			     Context *oncommit);
    using WritebackHandler::write;

    virtual void overwrite_extent(const object_t& oid, uint64_t off,
				  uint64_t len, ceph_tid_t original_journal_tid,
                                  ceph_tid_t new_journal_tid);

    virtual void get_client_lock();
    virtual void put_client_lock();

    struct write_result_d {
      bool done;
      int ret;
      std::string oid;
      Context *oncommit;
      write_result_d(const std::string& oid, Context *oncommit) :
	done(false), ret(0), oid(oid), oncommit(oncommit) {}
    private:
      write_result_d(const write_result_d& rhs);
      const write_result_d& operator=(const write_result_d& rhs);
    };

  private:
    void complete_writes(const std::string& oid);

    ceph_tid_t m_tid;
    Mutex& m_lock;
    librbd::ImageCtx *m_ictx;
    ceph::unordered_map<std::string, std::queue<write_result_d*> > m_writes;
    friend class C_OrderedWrite;
  };
}

#endif
