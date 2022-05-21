#!/bin/bash

cd ${0%/*}

[ -x "$(command -v rpmspec)" ] || {
    echo "No rpmspec binary was found." >&2 
    exit -1
}

reset_local_scripts() {
    local dir=$1
    local spec=$2
    local type=${1%%[0-9]*}
    spec="../rpm/${spec}.spec.src"

    [ -d $dir -a -f $spec ] || return 1

    specfile="$(sed -e "s,%%DATE%%,$(date +"%a %b %d %Y"),g" < $spec )"
    specfile="$(sed -e 's,%%\([A-Z_]\+\)%%,++\1++,g' <<<"$specfile" )"

    rpmspec <<<"$specfile" -q --qf '%{PREIN}' /dev/stdin |
       sed -e 's,(none),,g' > $dir/preinst.src

    rpmspec <<<"$specfile" -q --qf '%{POSTTRANS}' /dev/stdin |
       sed -e 's,(none),,g' > $dir/postinst.src

    rpmspec <<<"$specfile" -q --qf '%{PREUN}' /dev/stdin |
       sed -e 's,(none),,g' > $dir/prerm.src

    rpmspec <<<"$specfile" -q --qf '%{POSTUN}' /dev/stdin |
       sed -e 's,(none),,g' > $dir/postrm.src

    sed -i -e 's,++\([A-Z_]\+\)++,%\1%,g' -e 's,%,%%,g' $dir/preinst.src $dir/postinst.src $dir/prerm.src $dir/postrm.src
    find $dir/preinst.src $dir/postinst.src $dir/prerm.src $dir/postrm.src -empty -delete 

    return 0
}

reset_local_scripts client amanda-client
reset_local_scripts server amanda
