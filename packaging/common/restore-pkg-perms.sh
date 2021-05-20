#!/bin/bash
# $* == <tag-name> <dirs-with-rpm-stubs> ....

#set -xv

selfpath=$(realpath $0)
export PATH=${selfpath%/*}:/opt/zmanda/amanda/bin:${PATH}

# tags can equal:
#       PREIN, POSTTRANS, PREUN, POSTUN, VERIFYSCRIPT

for i do
    case "$i" in
      -*) opts+=("$i");;
      *) args+=("$i");;
    esac
done

if [ -x "$(command -v rpm)" ] && rpm -q -f /etc/os-release 2>/dev/null >&2; then
    OSTYPE=RPM
elif [ -x "$(command -v dpkg-query)" ] && dpkg-query -L base-files 2>/dev/null | grep -q /etc/os-release; then
    OSTYPE=DEB
fi

[ -z "$OSTYPE" ] &&
    die "could not find rpm or dpkg-query in restoring file permissions"

# uppercase whatever is there!
die() {
    echo "$@" 1>&2
    exit 100
}

add_manifest_rpm() {
    local i=$1
    local nm
    local manifest
    local manif
    # change to installed package name
    [ -r "$i" ] && i=$(rpm -q --qf '%{NAME}' -p $i)
    # change get canonical package name
    nm=$(rpm -q --qf '%{NAME}' $i)
    # make generic to debian/rpm
    nm=${nm//_/-}
    # need to find only installed manifests
    manifest=/opt/zmanda/amanda/pkg-manifests/${nm}.manifest
    [ -n "$(realpath -qe $manifest)" ]       && manif="${manifest}"
    [ -n "$(realpath -qe $manifest.preun)" ] && manif="${manifest}.preun"
    manif="$(realpath -qe $manif)" 
    [ -n "$manif" ] || die "ERROR: manifest for arg not found: $i -> $manifest (or .preun)"
    manfs+=($manif)
}

add_manifest_deb() {
    local i=$1
    local nm
    local manifest
    local manif

    # change to installed package name
    [ -r "$i" ] && i=$(dpkg-deb -W --showformat '${Package}' $i)
    # change get canonical package name
    nm="${i%%[ 	]*}"
    # need to find only installed manifests
    manifest=$(dpkg-query -L $nm | grep "/opt/zmanda/amanda/pkg-manifests/${nm}.manifest")
    [ -n "$(realpath -qe $manifest)" ]       && manif="${manifest}"
    [ -n "$(realpath -qe $manifest.preun)" ] && manif="${manifest}.preun"
    manif="$(realpath -qe $manif)" 
    [ -n "$manif" ] || die "ERROR: manifest for arg not found: $i -> $manifest (or .preun)"
    manfs+=($manif)
}

shell_clr_symlinks_rpm() {
    :
}

shell_clr_symlinks_deb() {
    :
}

shell_conf_save_rpm() {
    local nm=$1
    find 2>/dev/null /opt/zmanda/amanda/*-pkg -name ${nm}.rpm |
       while read i; do
           rpm -q -c -p $i
       done |
       while read i; do
           [ -e "$i" -a -s "$i" ] && mv -f "$i" "$i.cfgsave"
       done
}

shell_conf_save_deb() {
    local nm=$1
    find 2>/dev/null /opt/zmanda/amanda/*-pkg -name ${nm}.deb |
       while read i; do
           dpkg-deb --ctrl-tarfile $i | tar -xOf - ./conffiles;
       done |
       while read i; do
           [ -e "$i" -a -s "$i" ] && mv -f "$i" "$i.cfgsave"
       done
}

[ $(id -u) = 0 ] || \
   die "Root permissions are missing"

manfs=()

for i in "${args[@]}"; do
    [ $OSTYPE = RPM ] && add_manifest_rpm $i;
    [ $OSTYPE = DEB ] && add_manifest_deb $i;
done

total=${#manfs[@]}
[ $((total)) -gt 0 ] ||
    die "no manifests found: ${args[*]}"

if [ ! -x /opt/zmanda/amanda/python/bin/python3.6 ]; then
    for i in ${manfs[@]}; do
        nm="${i##*/}"
        nm="${nm%.manifest}"
        nm="${nm%.manifest.preun}"
        # erase any competing/misplaced directories/files
        [[ "x${opts[*]}" == x*--clr-symlinks* ]] && [ $OSTYPE = RPM ] &&
           shell_clr_symlinks_rpm $nm
        [[ "x${opts[*]}" == x*--clr-symlinks* ]] && [ $OSTYPE = DEB ] &&
           shell_clr_symlinks_deb $nm

        if [[ "x${opts[*]}" == x*--conf-save* ]]; then
            [ $OSTYPE = RPM ] && shell_conf_save_rpm $nm
            [ $OSTYPE = DEB ] && shell_conf_save_deb $nm
        fi
    done

    echo "INFO: skipped manifest-restore init: ${manfs[*]}" >&2
    exit 0
fi

for i in ${manfs[@]}; do
    if ! [[ "$i" == /opt/zmanda/amanda/pkg-manifests/*.manifest* ]]; then
        echo "ERROR: Invalid manifest file path: $i" >&2
        continue
    fi
    if ! [ -r "$i" -a -s "$i" ]; then
        echo "ERROR: Cannot read manifest file data: ${i##*/}" >&2
        continue
    fi
    #
    # FIXME: md5-verify actual manifest file in manifest-restore.. and detect first
    #
    # NOTE: MUST WORK WITH ZERO SYMLINKS AT ALL AT UNPACK!
    export PYTHONDONTWRITEBYTECODE=dont
    echo ----------- ${selfpath%/*}/manifest-restore "${opts[@]}" $i >&2
    ${selfpath%/*}/manifest-restore "${opts[@]}" $i
    r=$?
    case $((r)) in
       0) : $(( --total ))
          ;;
       *) echo "ERROR: Attempted files permissions/owners restore: st=$r: \"manifest-restore ${opts[@]} $i\"" >&2
          ;;
    esac
done

exit $total
