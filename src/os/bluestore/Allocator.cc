// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Allocator.h"
#include "StupidAllocator.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_bluestore

Allocator *Allocator::create(string type)
{
  if (type == "stupid")
    return new StupidAllocator;
  derr << "Allocator::" << __func__ << " unknown alloc type " << type << dendl;
  return NULL;
}
