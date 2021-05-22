#!/bin/bash

umask 022  # critical at times for root or other users!

files=("$@");
CWD=$PWD

[ -d debbuild/amanda-enterprise-*/installcheck ] || exit -1
INSTALLCHECK=$(ls -1d debbuild/amanda-enterprise-*/installcheck)

. ./packaging/common/build_functions.sh || { echo "ERROR: did not find ./packaging/... environment"; }
. ./packaging/common/test_functions.sh || { echo "ERROR: did not find ./packaging/... environment"; }

overtmp=${src_root}/debbuild/TMP
[ -d ${src_root}/../../debbuild -a -n "$(ls -d1 "${src_root}/../../debbuild/amanda-enterprise-*/debian" 2>/dev/null)" ] && 
    overtmp=${src_root%/*}/TMP
overstem=$overtmp/mnt

trap umount_all_overlays 0
( umount_all_overlays $overstem; )

#
# prep the test to run, first of all...
#
( 
cd $INSTALLCHECK
LOGNAME=$(stat -c %U .)
#su $LOGNAME -s /bin/bash -c 'touch Makefile.am && make clean && make -j`nproc` && ( make installcheck; true; )'
su $LOGNAME -s /bin/bash -c 'touch Makefile.am && make -j`nproc` && ( make installcheck; true; )'
) || exit -1

mount_overlays $overstem || exit -1
# generates "mnttop" variable ...

[ -n "$mnttop" -a -d "$mnttop" ] || exit -1;

df $mnttop/tmp/.

echo ................................ installing
cd $CWD
chroot_install $mnttop "${files[@]}" || exit -1
echo ................................ testing

( 
cd $INSTALLCHECK
chroot_amandabackup_cmd $mnttop 'make CLOBBER_MY_CONFIG=OK installcheck';
) || exit -1
echo ................................ done
