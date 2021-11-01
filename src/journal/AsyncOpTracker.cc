﻿/*
 Copyright (C) 2021 XTAO Technology <peng.hse@xtaotech.com>.
*/

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "journal/AsyncOpTracker.h"
#include "journal/Utils.h"
#include "include/assert.h"

namespace journal {

AsyncOpTracker::AsyncOpTracker()
  : m_lock(utils::unique_lock_name("AsyncOpTracker::m_lock", this)),
    m_pending_ops(0) {
}

AsyncOpTracker::~AsyncOpTracker() {
  wait_for_ops();
}

void AsyncOpTracker::start_op() {
  Mutex::Locker locker(m_lock);
  ++m_pending_ops;
}

void AsyncOpTracker::finish_op() {
  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);
    assert(m_pending_ops > 0);
    if (--m_pending_ops == 0) {
      m_cond.Signal();
      std::swap(on_finish, m_on_finish);
    }
  }

  if (on_finish != nullptr) {
    on_finish->complete(0);
  }
}

void AsyncOpTracker::wait_for_ops() {
  Mutex::Locker locker(m_lock);
  while (m_pending_ops > 0) {
    m_cond.Wait(m_lock);
  }
}

void AsyncOpTracker::wait_for_ops(Context *on_finish) {
  {
    Mutex::Locker locker(m_lock);
    assert(m_on_finish == nullptr);
    if (m_pending_ops > 0) {
      m_on_finish = on_finish;
      return;
    }
  }
  on_finish->complete(0);
}

bool AsyncOpTracker::empty() {
  Mutex::Locker locker(m_lock);
  return (m_pending_ops == 0);
}

} // namespace journal
