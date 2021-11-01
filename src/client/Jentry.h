// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLIENT_JENTRY_H
#define CEPH_CLIENT_JENTRY_H

#include <stdlib.h> // free()

typedef unsigned char uuid_t[16];

typedef struct mh_obj_id{
  uint64_t fs_key;
  ino_t inode;
  int validator;
  uuid_t uuid;
} mh_obj_id_t;

typedef struct mh_attr {
  mh_obj_id_t parentid;
  mh_obj_id_t fid;
  int depth;
  int dircount;
  mode_t    mode;    /* protection */
  nlink_t   nlink;   /* number of hard links */
  uid_t     uid;     /* user ID of owner */
  gid_t     gid;     /* group ID of owner */
  off_t     size;    /* total size, in bytes */
  blksize_t blksize; /* blocksize for file system I/O */
  blkcnt_t  blocks;  /* number of 512B blocks allocated */
  time_t    atime;   /* time of last access */
  time_t    mtime;   /* time of last modification */
  time_t    ctime;   /* time of last status change */
} mh_attr_t; 

typedef enum {
  mh_op_create = 0,
  mh_op_unlink,
  mh_op_rename,
  mh_op_mkdir,
  mh_op_rmdir,
  mh_op_setattr,
  mh_op_none,
  mh_op_done
} mh_op_type_t;

typedef struct mh_journal_entry {
  uint64_t  seq;
  uint64_t  len;
  mh_op_type_t op;
  char *name;
  char *name2; /* reserved for rename */
  mh_attr_t *attr;
  mh_attr_t *attr2;
  mh_attr_t *pattr;
  mh_attr_t *pattr2; /* reserved for rename */
  void *xattr; /* xt_map_t encoded */
  mh_journal_entry() : seq(0), op(mh_op_none), name(NULL),
		       name2(NULL), attr(NULL), attr2(NULL), pattr(NULL), pattr2(NULL),
		       xattr(NULL) { }
  ~mh_journal_entry() {
    if (attr) {
      delete attr;
      attr = NULL;
    }
    if (attr2) {
      delete attr2;
      attr2 = NULL;
    }
    if (pattr) {
      delete pattr;
      pattr = NULL;
    }
    if (pattr2) {
      delete pattr2;
      pattr2 = NULL;
    }
    if (name) {
      delete [] name;
      name = NULL;
    }
    if (name2) {
      delete [] name2;
      name2 = NULL;
    }
    if (xattr) {
      free(xattr);
      xattr = NULL;
    }
  }
} mh_journal_entry_t;



#endif
