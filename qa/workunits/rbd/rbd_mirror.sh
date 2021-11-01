#!/bin/sh
#
# rbd_mirror.sh - test rbd-mirror daemon
#
# The scripts starts two ("local" and "remote") clusters using mstart.sh script,
# creates a temporary directory, used for cluster configs, daemon logs, admin
# socket, temporary files, and launches rbd-mirror daemon.
#
# There are several env variables useful when troubleshooting a test failure:
#
#  RBD_MIRROR_NOCLEANUP - if not empty, don't run the cleanup (stop processes,
#                         destroy the clusters and remove the temp directory)
#                         on exit, so it is possible to check the test state
#                         after failure.
#  RBD_MIRROR_TEMDIR    - use this path when creating the temporary directory
#                         (should not exist) instead of running mktemp(1).
#
# The cleanup can be done as a separate step, running the script with
# `cleanup ${RBD_MIRROR_TEMDIR}' arguments.
#
# Note, as other workunits tests, rbd_mirror.sh expects to find ceph binaries
# in PATH.
#
# Thus a typical troubleshooting session:
#
# From Ceph src dir (CEPH_SRC_PATH), start the test in NOCLEANUP mode and with
# TEMPDIR pointing to a known location:
#
#   cd $CEPH_SRC_PATH
#   PATH=$CEPH_SRC_PATH:$PATH
#   RBD_MIRROR_NOCLEANUP=1 RBD_MIRROR_TEMDIR=/tmp/tmp.rbd_mirror \
#     ../qa/workunits/rbd/rbd_mirror.sh
#
# After the test failure cd to TEMPDIR and check the current state:
#
#   cd /tmp/tmp.rbd_mirror
#   ls
#   less rbd-mirror.cluster1_daemon.$pid.log
#   ceph --cluster cluster1 -s
#   ceph --cluster cluster1 -s
#   rbd --cluster cluster2 -p mirror ls
#   rbd --cluster cluster2 -p mirror journal status --image test
#   ceph --admin-daemon rbd-mirror.cluster1_daemon.cluster1.$pid.asok help
#   ...
#
# Also you can execute commands (functions) from the script:
#
#   cd $CEPH_SRC_PATH
#   export RBD_MIRROR_TEMDIR=/tmp/tmp.rbd_mirror
#   ../qa/workunits/rbd/rbd_mirror.sh status
#   ../qa/workunits/rbd/rbd_mirror.sh stop_mirror cluster1
#   ../qa/workunits/rbd/rbd_mirror.sh start_mirror cluster2
#   ../qa/workunits/rbd/rbd_mirror.sh flush cluster2
#   ...
#
# Eventually, run the cleanup:
#
#   cd $CEPH_SRC_PATH
#   RBD_MIRROR_TEMDIR=/tmp/tmp.rbd_mirror \
#     ../qa/workunits/rbd/rbd_mirror.sh cleanup
#

CLUSTER1=cluster1
CLUSTER2=cluster2
POOL=mirror
PARENT_POOL=mirror_parent
SRC_DIR=$(readlink -f $(dirname $0)/../../../src)
TEMPDIR=

# These vars facilitate running this script in an environment with
# ceph installed from packages, like teuthology. These are not defined
# by default.
#
# RBD_MIRROR_USE_EXISTING_CLUSTER - if set, do not start and stop ceph clusters
# RBD_MIRROR_USE_RBD_MIRROR - if set, use an existing instance of rbd-mirror
#                             running as ceph client $CEPH_ID. If empty,
#                             this script will start and stop rbd-mirror

#
# Functions
#

daemon_asok_file()
{
    local local_cluster=$1
    local cluster=$2

    if [ -n "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
        echo $(ceph-conf --cluster $local_cluster --name "client.${CEPH_ID}" 'admin socket')
    else
        echo "${TEMPDIR}/rbd-mirror.${local_cluster}_daemon.${cluster}.asok"
    fi
}

daemon_pid_file()
{
    local cluster=$1

    if [ -n "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
        echo $(ceph-conf --cluster $cluster --name "client.${CEPH_ID}" 'pid file')
    else
        echo "${TEMPDIR}/rbd-mirror.${cluster}_daemon.pid"
    fi
}

testlog()
{
    echo $(date '+%F %T') $@ | tee -a "${TEMPDIR}/rbd-mirror.test.log"
}

setup()
{
    local c
    trap cleanup INT TERM EXIT

    if [ -n "${RBD_MIRROR_TEMDIR}" ]; then
	mkdir "${RBD_MIRROR_TEMDIR}"
	TEMPDIR="${RBD_MIRROR_TEMDIR}"
    else
	TEMPDIR=`mktemp -d`
    fi

    if [ -z "${RBD_MIRROR_USE_EXISTING_CLUSTER}" ]; then
        cd ${SRC_DIR}
        ./mstart.sh ${CLUSTER1} -n
        ./mstart.sh ${CLUSTER2} -n

        ln -s $(readlink -f run/${CLUSTER1}/ceph.conf) \
           ${TEMPDIR}/${CLUSTER1}.conf
        ln -s $(readlink -f run/${CLUSTER2}/ceph.conf) \
           ${TEMPDIR}/${CLUSTER2}.conf

        cd ${TEMPDIR}
    fi

    ceph --cluster ${CLUSTER1} osd pool create ${POOL} 64 64
    ceph --cluster ${CLUSTER1} osd pool create ${PARENT_POOL} 64 64
    ceph --cluster ${CLUSTER2} osd pool create ${PARENT_POOL} 64 64
    ceph --cluster ${CLUSTER2} osd pool create ${POOL} 64 64

    rbd --cluster ${CLUSTER1} mirror pool enable ${POOL} pool
    rbd --cluster ${CLUSTER2} mirror pool enable ${POOL} pool
    rbd --cluster ${CLUSTER1} mirror pool enable ${PARENT_POOL} image
    rbd --cluster ${CLUSTER2} mirror pool enable ${PARENT_POOL} image

    rbd --cluster ${CLUSTER1} mirror pool peer add ${POOL} ${CLUSTER2}
    rbd --cluster ${CLUSTER2} mirror pool peer add ${POOL} ${CLUSTER1}
    rbd --cluster ${CLUSTER1} mirror pool peer add ${PARENT_POOL} ${CLUSTER2}
    rbd --cluster ${CLUSTER2} mirror pool peer add ${PARENT_POOL} ${CLUSTER1}
}

cleanup()
{
    test  -n "${RBD_MIRROR_NOCLEANUP}" && return

    set +e

    stop_mirror "${CLUSTER1}"
    stop_mirror "${CLUSTER2}"

    if [ -z "${RBD_MIRROR_USE_EXISTING_CLUSTER}" ]; then
        cd ${SRC_DIR}
        ./mstop.sh ${CLUSTER1}
        ./mstop.sh ${CLUSTER2}
    else
        ceph --cluster ${CLUSTER1} osd pool rm ${POOL} ${POOL} --yes-i-really-really-mean-it
        ceph --cluster ${CLUSTER2} osd pool rm ${POOL} ${POOL} --yes-i-really-really-mean-it
        ceph --cluster ${CLUSTER1} osd pool rm ${PARENT_POOL} ${PARENT_POOL} --yes-i-really-really-mean-it
        ceph --cluster ${CLUSTER2} osd pool rm ${PARENT_POOL} ${PARENT_POOL} --yes-i-really-really-mean-it
    fi
    rm -Rf ${TEMPDIR}
}

start_mirror()
{
    local cluster=$1

    test -n "${RBD_MIRROR_USE_RBD_MIRROR}" && return

    rbd-mirror \
	--cluster ${cluster} \
	--pid-file=$(daemon_pid_file "${cluster}") \
	--log-file=${TEMPDIR}/rbd-mirror.${cluster}_daemon.\$cluster.\$pid.log \
	--admin-socket=${TEMPDIR}/rbd-mirror.${cluster}_daemon.\$cluster.asok \
	--debug-rbd=30 --debug-journaler=30 \
	--debug-rbd_mirror=30 \
	--daemonize=true
}

stop_mirror()
{
    local cluster=$1

    test -n "${RBD_MIRROR_USE_RBD_MIRROR}" && return

    local pid
    pid=$(cat $(daemon_pid_file "${cluster}") 2>/dev/null) || :
    if [ -n "${pid}" ]
    then
	kill ${pid}
	for s in 1 2 4 8 16 32; do
	    sleep $s
	    ps auxww | awk -v pid=${pid} '$2 == pid {print; exit 1}' && break
	done
	ps auxww | awk -v pid=${pid} '$2 == pid {print; exit 1}'
    fi
    rm -f $(daemon_asok_file "${cluster}" "${CLUSTER1}")
    rm -f $(daemon_asok_file "${cluster}" "${CLUSTER2}")
    rm -f $(daemon_pid_file "${cluster}")
}

admin_daemon()
{
    local cluster=$1 ; shift

    local asok_file=$(daemon_asok_file "${cluster}" "${cluster}")
    test -S "${asok_file}"

    ceph --admin-daemon ${asok_file} $@
}

status()
{
    local cluster daemon image_pool image

    for cluster in ${CLUSTER1} ${CLUSTER2}
    do
	echo "${cluster} status"
	ceph --cluster ${cluster} -s
	echo

	for image_pool in ${POOL} ${PARENT_POOL}
	do
	    echo "${cluster} ${image_pool} images"
	    rbd --cluster ${cluster} -p ${image_pool} ls
	    echo

	    echo "${cluster} ${image_pool} mirror pool status"
	    rbd --cluster ${cluster} -p ${image_pool} mirror pool status --verbose
	    echo

	    for image in `rbd --cluster ${cluster} -p ${image_pool} ls 2>/dev/null`
	    do
	        echo "image ${image} info"
	        rbd --cluster ${cluster} -p ${image_pool} info ${image}
	        echo
	        echo "image ${image} journal status"
	        rbd --cluster ${cluster} -p ${image_pool} journal status --image ${image}
	        echo
	    done
	done
    done

    local ret

    for cluster in "${CLUSTER1}" "${CLUSTER2}"
    do
	local pid_file=$(daemon_pid_file ${cluster} )
	if [ ! -e ${pid_file} ]
	then
	    echo "${cluster} rbd-mirror not running or unknown" \
		 "(${pid_file} not exist)"
	    continue
	fi

	local pid
	pid=$(cat ${pid_file} 2>/dev/null) || :
	if [ -z "${pid}" ]
	then
	    echo "${cluster} rbd-mirror not running or unknown" \
		 "(can't find pid using ${pid_file})"
	    ret=1
	    continue
	fi

	echo "${daemon} rbd-mirror process in ps output:"
	if ps auxww |
		awk -v pid=${pid} 'NR == 1 {print} $2 == pid {print; exit 1}'
	then
	    echo
	    echo "${cluster} rbd-mirror not running" \
		 "(can't find pid $pid in ps output)"
	    ret=1
	    continue
	fi
	echo

	local asok_file=$(daemon_asok_file ${cluster} ${cluster})
	if [ ! -S "${asok_file}" ]
	then
	    echo "${cluster} rbd-mirror asok is unknown (${asok_file} not exits)"
	    ret=1
	    continue
	fi

	echo "${cluster} rbd-mirror status"
	ceph --admin-daemon ${asok_file} rbd mirror status
	echo
    done

    return ${ret}
}

flush()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local cmd="rbd mirror flush"

    if [ -n "${image}" ]
    then
       cmd="${cmd} ${pool}/${image}"
    fi

    admin_daemon "${cluster}" ${cmd}
}

test_image_replay_state()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local test_state=$4
    local current_state=stopped

    admin_daemon "${cluster}" help |
	fgrep "\"rbd mirror status ${pool}/${image}\"" &&
    admin_daemon "${cluster}" rbd mirror status ${pool}/${image} |
	grep -i 'state.*Replaying' &&
    current_state=started

    test "${test_state}" = "${current_state}"
}

wait_for_image_replay_state()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local state=$4
    local s

    # TODO: add a way to force rbd-mirror to update replayers
    for s in 1 2 4 8 8 8 8 8 8 8 8 16 16; do
	sleep ${s}
	test_image_replay_state "${cluster}" "${pool}" "${image}" "${state}" && return 0
    done
    return 1
}

wait_for_image_replay_started()
{
    local cluster=$1
    local pool=$2
    local image=$3

    wait_for_image_replay_state "${cluster}" "${pool}" "${image}" started
}

wait_for_image_replay_stopped()
{
    local cluster=$1
    local pool=$2
    local image=$3

    wait_for_image_replay_state "${cluster}" "${pool}" "${image}" stopped
}

get_position()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local id_regexp=$4

    # Parse line like below, looking for the first position
    # [id=, commit_position=[positions=[[object_number=1, tag_tid=3, entry_tid=9], [object_number=0, tag_tid=3, entry_tid=8], [object_number=3, tag_tid=3, entry_tid=7], [object_number=2, tag_tid=3, entry_tid=6]]]]

    local status_log=${TEMPDIR}/${CLUSTER2}-${pool}-${image}.status
    rbd --cluster ${cluster} -p ${pool} journal status --image ${image} |
	tee ${status_log} >&2
    sed -nEe 's/^.*\[id='"${id_regexp}"',.*positions=\[\[([^]]*)\],.*$/\1/p' \
	${status_log}
}

get_master_position()
{
    local cluster=$1
    local pool=$2
    local image=$3

    get_position "${cluster}" "${pool}" "${image}" ''
}

get_mirror_position()
{
    local cluster=$1
    local pool=$2
    local image=$3

    get_position "${cluster}" "${pool}" "${image}" '..*'
}

wait_for_replay_complete()
{
    local local_cluster=$1
    local cluster=$2
    local pool=$3
    local image=$4
    local s master_pos mirror_pos

    for s in 0.2 0.4 0.8 1.6 2 2 4 4 8 8 16 16; do
	sleep ${s}
	flush "${local_cluster}" "${pool}" "${image}"
	master_pos=$(get_master_position "${cluster}" "${pool}" "${image}")
	mirror_pos=$(get_mirror_position "${cluster}" "${pool}" "${image}")
	test -n "${master_pos}" -a "${master_pos}" = "${mirror_pos}" && return 0
    done
    return 1
}

test_status_in_pool_dir()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local state_pattern=$4
    local description_pattern=$5

    local status_log=${TEMPDIR}/${cluster}-${image}.mirror_status
    rbd --cluster ${cluster} -p ${pool} mirror image status ${image} |
	tee ${status_log}
    grep "state: .*${state_pattern}" ${status_log}
    grep "description: .*${description_pattern}" ${status_log}
}

create_image()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster ${cluster} -p ${pool} create --size 128 \
	--image-feature layering,exclusive-lock,journaling ${image}
}

remove_image()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster ${cluster} -p ${pool} rm ${image}
}

clone_image()
{
    local cluster=$1
    local parent_pool=$2
    local parent_image=$3
    local parent_snap=$4
    local clone_pool=$5
    local clone_image=$6

    rbd --cluster ${cluster} clone ${parent_pool}/${parent_image}@${parent_snap} \
	${clone_pool}/${clone_image} --image-feature layering,exclusive-lock,journaling
}

create_snapshot()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local snap=$4

    rbd --cluster ${cluster} -p ${pool} snap create ${image}@${snap}
}

remove_snapshot()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local snap=$4

    rbd --cluster ${cluster} -p ${pool} snap rm ${image}@${snap}
}

protect_snapshot()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local snap=$4

    rbd --cluster ${cluster} -p ${pool} snap protect ${image}@${snap}
}

unprotect_snapshot()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local snap=$4

    rbd --cluster ${cluster} -p ${pool} snap unprotect ${image}@${snap}
}

wait_for_snap_present()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local snap_name=$4
    local s

    for s in 1 2 4 8 8 8 8 8 8 8 8 16 16 16 16 32 32 32 32; do
	sleep ${s}
        rbd --cluster ${cluster} -p ${pool} info ${image}@${snap_name} || continue
        return 0
    done
    return 1
}

write_image()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local count=$4

    rbd --cluster ${cluster} -p ${pool} bench-write ${image} \
	--io-size 4096 --io-threads 1 --io-total $((4096 * count)) \
	--io-pattern rand
}

compare_images()
{
    local pool=$1
    local image=$2

    local rmt_export=${TEMPDIR}/${CLUSTER2}-${pool}-${image}.export
    local loc_export=${TEMPDIR}/${CLUSTER1}-${pool}-${image}.export

    rm -f ${rmt_export} ${loc_export}
    rbd --cluster ${CLUSTER2} -p ${pool} export ${image} ${rmt_export}
    rbd --cluster ${CLUSTER1} -p ${pool} export ${image} ${loc_export}
    cmp ${rmt_export} ${loc_export}
}

demote_image()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster=${cluster} mirror image demote ${pool}/${image}
}

promote_image()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster=${cluster} mirror image promote ${pool}/${image}
}

set_pool_mirror_mode()
{
    local cluster=$1
    local pool=$2
    local mode=$3

    rbd --cluster=${cluster} -p ${pool} mirror pool enable ${mode}
}

disable_mirror()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster=${cluster} mirror image disable ${pool}/${image}
}

enable_mirror()
{
    local cluster=$1
    local pool=$2
    local image=$3

    rbd --cluster=${cluster} mirror image enable ${pool}/${image}
}

test_image_present()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local test_state=$4
    local current_state=deleted

    rbd --cluster=${cluster} -p ${pool} ls | grep "^${image}$" &&
    current_state=present

    test "${test_state}" = "${current_state}"
}

wait_for_image_present()
{
    local cluster=$1
    local pool=$2
    local image=$3
    local state=$4
    local s

    # TODO: add a way to force rbd-mirror to update replayers
    for s in 1 2 4 8 8 8 8 8 8 8 8 16 16; do
	sleep ${s}
	test_image_present "${cluster}" "${pool}" "${image}" "${state}" && return 0
    done
    return 1
}

#
# Main
#

if [ "$#" -gt 0 ]
then
    if [ -z "${RBD_MIRROR_TEMDIR}" ]
    then
       echo "RBD_MIRROR_TEMDIR is not set" >&2
       exit 1
    fi

    TEMPDIR="${RBD_MIRROR_TEMDIR}"
    cd ${TEMPDIR}
    $@
    exit $?
fi

set -xe

setup

testlog "TEST: add image and test replay"
start_mirror ${CLUSTER1}
image=test
create_image ${CLUSTER2} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
write_image ${CLUSTER2} ${POOL} ${image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${image}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying' 'master_position'
if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  test_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'down+unknown'
fi
compare_images ${POOL} ${image}

testlog "TEST: stop mirror, add image, start mirror and test replay"
stop_mirror ${CLUSTER1}
image1=test1
create_image ${CLUSTER2} ${POOL} ${image1}
write_image ${CLUSTER2} ${POOL} ${image1} 100
start_mirror ${CLUSTER1}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${image1}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image1} 'up+replaying' 'master_position'
if [ -z "${RBD_MIRROR_USE_RBD_MIRROR}" ]; then
  test_status_in_pool_dir ${CLUSTER2} ${POOL} ${image1} 'down+unknown'
fi
compare_images ${POOL} ${image1}

testlog "TEST: test the first image is replaying after restart"
write_image ${CLUSTER2} ${POOL} ${image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${image}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying' 'master_position'
compare_images ${POOL} ${image}

testlog "TEST: stop/start/restart mirror via admin socket"
admin_daemon ${CLUSTER1} rbd mirror stop
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image1}

admin_daemon ${CLUSTER1} rbd mirror start
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}

admin_daemon ${CLUSTER1} rbd mirror restart
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}

admin_daemon ${CLUSTER1} rbd mirror stop
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image1}

admin_daemon ${CLUSTER1} rbd mirror restart
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}

admin_daemon ${CLUSTER1} rbd mirror stop ${POOL} ${CLUSTER2}
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image1}

admin_daemon ${CLUSTER1} rbd mirror start ${POOL}/${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}

admin_daemon ${CLUSTER1} rbd mirror start
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}

admin_daemon ${CLUSTER1} rbd mirror start ${POOL} ${CLUSTER2}

admin_daemon ${CLUSTER1} rbd mirror restart ${POOL}/${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}

admin_daemon ${CLUSTER1} rbd mirror restart ${POOL} ${CLUSTER2}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image1}

admin_daemon ${CLUSTER1} rbd mirror flush
admin_daemon ${CLUSTER1} rbd mirror status

testlog "TEST: failover and failback"
start_mirror ${CLUSTER2}

# failover
demote_image ${CLUSTER2} ${POOL} ${image}
wait_for_image_replay_stopped ${CLUSTER1} ${POOL} ${image}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+stopped'
test_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+stopped'
promote_image ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER2} ${POOL} ${image}
write_image ${CLUSTER1} ${POOL} ${image} 100
wait_for_replay_complete ${CLUSTER2} ${CLUSTER1} ${POOL} ${image}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+stopped'
test_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+replaying' 'master_position'
compare_images ${POOL} ${image}

# failback
demote_image ${CLUSTER1} ${POOL} ${image}
wait_for_image_replay_stopped ${CLUSTER2} ${POOL} ${image}
test_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+stopped'
promote_image ${CLUSTER2} ${POOL} ${image}
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}
write_image ${CLUSTER2} ${POOL} ${image} 100
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${image}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${image} 'up+replaying' 'master_position'
test_status_in_pool_dir ${CLUSTER2} ${POOL} ${image} 'up+stopped'
compare_images ${POOL} ${image}

testlog "TEST: cloned images"
parent_image=test_parent
parent_snap=snap
create_image ${CLUSTER2} ${PARENT_POOL} ${parent_image}
write_image ${CLUSTER2} ${PARENT_POOL} ${parent_image} 100
create_snapshot ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap}
protect_snapshot ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap}

clone_image=test_clone
clone_image ${CLUSTER2} ${PARENT_POOL} ${parent_image} ${parent_snap} ${POOL} ${clone_image}
write_image ${CLUSTER2} ${POOL} ${clone_image} 100

enable_mirror ${CLUSTER2} ${PARENT_POOL} ${parent_image}
wait_for_image_replay_started ${CLUSTER1} ${PARENT_POOL} ${parent_image}
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${PARENT_POOL} ${parent_image}
test_status_in_pool_dir ${CLUSTER1} ${PARENT_POOL} ${parent_image} 'up+replaying' 'master_position'
compare_images ${PARENT_POOL} ${parent_image}

wait_for_image_replay_started ${CLUSTER1} ${POOL} ${clone_image}
wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${clone_image}
test_status_in_pool_dir ${CLUSTER1} ${POOL} ${clone_image} 'up+replaying' 'master_position'
compare_images ${POOL} ${clone_image}

testlog "TEST: disable mirroring / delete non-primary image"
image2=test2
image3=test3
image4=test4
image5=test5
for i in ${image2} ${image3} ${image4} ${image5}; do
  create_image ${CLUSTER2} ${POOL} ${i}
  write_image ${CLUSTER2} ${POOL} ${i} 100
  create_snapshot ${CLUSTER2} ${POOL} ${i} 'snap1'
  create_snapshot ${CLUSTER2} ${POOL} ${i} 'snap2'
  if [ "${i}" = "${image4}" ] || [ "${i}" = "${image5}" ]; then
    protect_snapshot ${CLUSTER2} ${POOL} ${i} 'snap1'
    protect_snapshot ${CLUSTER2} ${POOL} ${i} 'snap2'
  fi
  write_image ${CLUSTER2} ${POOL} ${i} 100
  wait_for_image_present ${CLUSTER1} ${POOL} ${i} 'present'
  wait_for_snap_present ${CLUSTER1} ${POOL} ${i} 'snap2'
done

set_pool_mirror_mode ${CLUSTER2} ${POOL} 'image'
for i in ${image2} ${image4}; do
  disable_mirror ${CLUSTER2} ${POOL} ${i}
done

unprotect_snapshot ${CLUSTER2} ${POOL} ${image5} 'snap1'
unprotect_snapshot ${CLUSTER2} ${POOL} ${image5} 'snap2'
for i in ${image3} ${image5}; do
  remove_snapshot ${CLUSTER2} ${POOL} ${i} 'snap1'
  remove_snapshot ${CLUSTER2} ${POOL} ${i} 'snap2'
  remove_image ${CLUSTER2} ${POOL} ${i}
done

for i in ${image2} ${image3} ${image4} ${image5}; do
  wait_for_image_present ${CLUSTER1} ${POOL} ${i} 'deleted'
done

set_pool_mirror_mode ${CLUSTER2} ${POOL} 'pool'
for i in ${image2} ${image4}; do
  wait_for_image_present ${CLUSTER1} ${POOL} ${i} 'present'
  wait_for_snap_present ${CLUSTER1} ${POOL} ${i} 'snap2'
  wait_for_image_replay_started ${CLUSTER1} ${POOL} ${i}
  wait_for_replay_complete ${CLUSTER1} ${CLUSTER2} ${POOL} ${i}
  compare_images ${POOL} ${i}
done

testlog "TEST: disable mirror while daemon is stopped"
stop_mirror ${CLUSTER1}
stop_mirror ${CLUSTER2}
set_pool_mirror_mode ${CLUSTER2} ${POOL} 'image'
disable_mirror ${CLUSTER2} ${POOL} ${image}
test_image_present ${CLUSTER1} ${POOL} ${image} 'present'
start_mirror ${CLUSTER1}
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'deleted'
set_pool_mirror_mode ${CLUSTER2} ${POOL} 'pool'
wait_for_image_present ${CLUSTER1} ${POOL} ${image} 'present'
wait_for_image_replay_started ${CLUSTER1} ${POOL} ${image}

echo OK
