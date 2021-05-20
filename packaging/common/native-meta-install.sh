#!/bin/bash 
# $* == <dirs-with-rpm-stubs>

declare -a args
declare -a opts

for i do
    case "$i" in
      -*) opts=("${opts[@]}" "$i");;
      *) args=("${args[@]}" "$i");;
    esac
done

unset -f pkg_names pkg_inst_names pkg_script

if [ -x "$(command -v rpm)" ] && rpm -q -f /etc/os-release 2>/dev/null >&2; then
##########################################################################################################

    function pkg_names() {
       find "$@" -path '*.rpm' -o -path '*/*.rpm' | grep '\.rpm$' |
          while read p; do
              rpm2cpio $p | grep -qam1 . && continue
              rpm -q --qf "%{NAME}\t$p\n" --nomanifest -p $p;
          done
    }

    function pkg_inst_names() {
        rpm -q --qf '%{NAME}\n' "$@" 2>/dev/null | grep -v ' ';
    }

    function pkg_script() {
        local tag=$1;
        local nm=$2;
        [ -z "$nm" ] && return
        if [ -r "${pkg_set[$nm]}" ]; then
            rpm -q --qf "%{$tag}" -p "${pkg_set[$nm]}" | grep -v '^(none)'
        else
            rpm -q --qf "%{$tag}" "$nm" | grep -v '^(none)'
        fi
    }

    function unpack_pkgs() {
        rpm 2>&1 --justdb -U --quiet --nodigest --noscripts --ignoreos ${opts[@]} "$@";
    }

    type="RPM"

##########################################################################################################
elif [ -x "$(command -v dpkg-query)" ] && dpkg-query -L base-files 2>/dev/null | grep -q /etc/os-release; then
    function pkg_names() {
       find "$@" -path '*.deb' -o -path '*/*.deb' | grep '\.deb$' |
          while read p; do
              # { ar p $p data.tar.gz | tar -tzf - | grep -q var/lib/dpkg/info/stable-add; } || continue
              dpkg-deb -W --showformat "\${Package}\t$p\n" $p;
          done
    }
    function pkg_inst_names() {
          xargs <<<"$@" -n1 dpkg-query -W --showformat '${Package}\n' 2>/dev/null
    }
    function pkg_script() {
        local debscript=$1;
        local nm=$2;
        [ -z "$nm" -o -z "$debscript" ] && return

        if [ -r "${pkg_set[$nm]}" ]; then
            dpkg-deb --ctrl-tarfile "${pkg_set[$nm]}" | tar -xOvf - ./$debscript;
        else
            [ -r "/var/lib/dpkg/info/$nm.$debscript" ] && cat "/var/lib/dpkg/info/$nm.$debscript"
        fi
    }

    function unpack_pkgs() {
        aptver="$(dpkg-query -W --showformat '${Version}' apt)"
        if [ ${opts[0]} = "--test" ] && [[ $aptver > 1.1 ]]; then
            apt-get install --no-download -s "$@" | grep -v 'Note, selecting.* instead of'
            return $?
        fi
        if [ ${opts[0]} = "--test" ]; then
            nms=($(for i do dpkg-deb -W -f '${Package}\n' $i; done))
            nms="$(IFS='|'; echo "${a[*]}")"
            for i do dpkg-query -f $i Depends; done | 
                sed -e 's/([^)]*)//g' -e 's/, */\n/g' | 
                egrep -v "$nms" |
                sort -u | xargs dpkg-query -W --showformat '${Status}\n'
            return $?
        fi
        dpkg --unpack "${opts[@]}" "$@"
    }

    # debian modes
    type="DEB"
    tag="${tag,,}"        # lowercase
    tag="${tag/un/rm}"    # chg un to rm
    tag="${tag/in/inst}"  # chg in to inst

##########################################################################################################
else
    echo 'Failed to detect Debian or RPM install system' | tee /dev/stderr
    exit 1
fi

die() {
    echo "$@" 1>&2
    exit 100
}

declare -A pkg_set
declare -A pkg_inst_cnt
declare -a instpkgs

[ $(id -u) = 0 ] || \
   die "Root permissions are missing"

 list="$(pkg_names $(ls 2>/dev/null -1d "${args[@]}"))"

# default adjust is zero
# [ $tag = PREIN ] && adj=1

while read pkg pkgpath; do
    pkg_inst_cnt[$pkg]=$(( $(pkg_inst_names $pkg | grep -c .) + adj ))
    pkg_set[$pkg]="$pkgpath"
done <<<"$list"

order_of_install=(\
   zmanda-platform-shared \
   amanda_enterprise-platform \
   amanda_enterprise-backup-server \
   amanda_enterprise-extensions-server \
   zmanda-ui-platform \
   aee-backup-platform \
   zmc-ui-platform \
   zmc-ui-nodejs \
   aee-backup-server \
   zmc-ui-rest \
   )


for pkg in "${order_of_install[@]}"; do
    [ -n "$pkg" ] || continue
    instpkgs=(${instpkgs[@]} ${pkg_set[$pkg]})
done

# dependencies and files must mesh correctly!!
unpack_pkgs "${instpkgs[@]}" 

[ $? = 0 ] && echo "--------------------- success ${args[@]}"

true
