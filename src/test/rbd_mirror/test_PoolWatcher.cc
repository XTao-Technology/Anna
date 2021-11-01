// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "include/stringify.h"
#include "cls/rbd/cls_rbd_client.h"
#include "include/rbd_types.h"
#include "librbd/internal.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "common/Mutex.h"
#include "tools/rbd_mirror/PoolWatcher.h"
#include "tools/rbd_mirror/types.h"
#include "test/librados/test.h"
#include "gtest/gtest.h"
#include <boost/scope_exit.hpp>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <vector>

using rbd::mirror::PoolWatcher;
using rbd::mirror::peer_t;
using rbd::mirror::RadosRef;
using std::map;
using std::set;
using std::string;

void register_test_pool_watcher() {
}

class TestPoolWatcher : public ::testing::Test {
public:

TestPoolWatcher() : m_lock("TestPoolWatcherLock"),
    m_image_number(0), m_snap_number(0)
  {
    m_cluster = std::make_shared<librados::Rados>();
    EXPECT_EQ("", connect_cluster_pp(*m_cluster));
  }

  ~TestPoolWatcher() {
    m_cluster->wait_for_latest_osdmap();
    for (auto& pool : m_pools) {
      EXPECT_EQ(0, m_cluster->pool_delete(pool.c_str()));
    }
  }

  void create_pool(bool enable_mirroring, const peer_t &peer, string *name=nullptr) {
    string pool_name = get_temp_pool_name("test-rbd-mirror-");
    ASSERT_EQ(0, m_cluster->pool_create(pool_name.c_str()));

    int64_t pool_id = m_cluster->pool_lookup(pool_name.c_str());
    ASSERT_GE(pool_id, 0);
    m_pools.insert(pool_name);

    librados::IoCtx ioctx;
    ASSERT_EQ(0, m_cluster->ioctx_create2(pool_id, ioctx));

    m_pool_watcher.reset(new PoolWatcher(ioctx, 30, m_lock, m_cond));
    if (enable_mirroring) {
      ASSERT_EQ(0, librbd::mirror_mode_set(ioctx, RBD_MIRROR_MODE_POOL));
      std::string uuid;
      ASSERT_EQ(0, librbd::mirror_peer_add(ioctx, &uuid,
					   peer.cluster_name,
					   peer.client_name));
    }
    if (name != nullptr) {
      *name = pool_name;
    }
  }

  string get_image_id(librados::IoCtx *ioctx, const string &image_name) {
    string obj = librbd::util::id_obj_name(image_name);
    string id;
    EXPECT_EQ(0, librbd::cls_client::get_id(ioctx, obj, &id));
    return id;
  }

  void create_image(const string &pool_name, bool mirrored=true,
		    string *image_name=nullptr) {
    uint64_t features = g_ceph_context->_conf->rbd_default_features;
    string name = "image" + stringify(++m_image_number);
    if (mirrored) {
      features |= RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING;
    }

    librados::IoCtx ioctx;
    ASSERT_EQ(0, m_cluster->ioctx_create(pool_name.c_str(), ioctx));
    int order = 0;
    ASSERT_EQ(0, librbd::create(ioctx, name.c_str(), 1 << 22, false,
				features, &order, 0, 0));
    if (mirrored) {
      librbd::Image image;
      librbd::RBD rbd;
      rbd.open(ioctx, image, name.c_str());
      image.mirror_image_enable();

      librbd::mirror_image_info_t mirror_image_info;
      ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image_info,
                                               sizeof(mirror_image_info)));
      image.close();

      m_mirrored_images.insert(PoolWatcher::ImageId(
        get_image_id(&ioctx, name), name, mirror_image_info.global_id));
    }
    if (image_name != nullptr)
      *image_name = name;
  }

  void clone_image(const string &parent_pool_name,
		   const string &parent_image_name,
		   const string &clone_pool_name,
		   bool mirrored=true,
		   string *image_name=nullptr) {
    librados::IoCtx pioctx, cioctx;
    ASSERT_EQ(0, m_cluster->ioctx_create(parent_pool_name.c_str(), pioctx));
    ASSERT_EQ(0, m_cluster->ioctx_create(clone_pool_name.c_str(), cioctx));

    string snap_name = "snap" + stringify(++m_snap_number);
    {
      librbd::ImageCtx *ictx = new librbd::ImageCtx(parent_image_name.c_str(),
						    "", "", pioctx, false);
      ictx->state->open();
      EXPECT_EQ(0, ictx->operations->snap_create(snap_name.c_str()));
      EXPECT_EQ(0, ictx->operations->snap_protect(snap_name.c_str()));
      ictx->state->close();
    }

    uint64_t features = g_ceph_context->_conf->rbd_default_features;
    string name = "clone" + stringify(++m_image_number);
    if (mirrored) {
      features |= RBD_FEATURE_EXCLUSIVE_LOCK | RBD_FEATURE_JOURNALING;
    }
    int order = 0;
    librbd::clone(pioctx, parent_image_name.c_str(), snap_name.c_str(),
		  cioctx, name.c_str(), features, &order, 0, 0);
    if (mirrored) {
      librbd::Image image;
      librbd::RBD rbd;
      rbd.open(cioctx, image, name.c_str());
      image.mirror_image_enable();

      librbd::mirror_image_info_t mirror_image_info;
      ASSERT_EQ(0, image.mirror_image_get_info(&mirror_image_info,
                                               sizeof(mirror_image_info)));
      image.close();

      m_mirrored_images.insert(PoolWatcher::ImageId(
        get_image_id(&cioctx, name), name, mirror_image_info.global_id));
    }
    if (image_name != nullptr)
      *image_name = name;
  }

  void check_images() {
    m_pool_watcher->refresh_images(false);
    Mutex::Locker l(m_lock);
    ASSERT_EQ(m_mirrored_images, m_pool_watcher->get_images());
  }

  Mutex m_lock;
  Cond m_cond;
  RadosRef m_cluster;
  unique_ptr<PoolWatcher> m_pool_watcher;

  set<string> m_pools;
  PoolWatcher::ImageIds m_mirrored_images;

  uint64_t m_image_number;
  uint64_t m_snap_number;
};

TEST_F(TestPoolWatcher, EmptyPool) {
  string uuid1 = "00000000-0000-0000-0000-000000000001";
  peer_t site1(uuid1, "site1", "mirror1");
  create_pool(true, site1);
  check_images();
}

TEST_F(TestPoolWatcher, ReplicatedPools) {
  string uuid1 = "00000000-0000-0000-0000-000000000001";
  peer_t site1(uuid1, "site1", "mirror1");
  string first_pool, local_pool, last_pool;
  create_pool(true, site1, &first_pool);
  check_images();
  create_image(first_pool);
  check_images();
  string parent_image, parent_image2;
  create_image(first_pool, true, &parent_image);
  check_images();
  clone_image(first_pool, parent_image, first_pool);
  check_images();
  clone_image(first_pool, parent_image, first_pool, true, &parent_image2);
  check_images();
  create_image(first_pool, false);
  check_images();
}
