#!/bin/bash 
# $* == <dirs-with-rpm-stubs>

die() {
    echo "$@" 1>&2
    exit 100
}

[ $(id -u) = 0 ] || \
   die "Root permissions are missing"

order_of_install=(\
   zmc-ui-rest \
   aee-backup-server \
   zmc-ui-nodejs \  
   zmc-ui-platform \
   aee-backup-platform \
   zmanda-ui-platform \
   amanda_enterprise-extensions-server \
   amanda_enterprise-backup-server \
   amanda_enterprise-platform \
   zmanda-platform-shared \
   )

#
# handle database update *before* any files have been uncompressed at all!!
#
for pkg in "${order_of_install[@]}"; do
    rpm -q "$pkg" | grep -v ' ' | tr '\n' ' '
done

true
