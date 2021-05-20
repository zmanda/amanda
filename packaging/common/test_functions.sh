#!/usr/bin/env bash

TOPDIR=/opt/zmanda/amanda

command -v realpath >/dev/null || realpath() { readlink -e "$@"; }

get_active_mounts() {
    local overbase=$1
    overbase="${overbase:-[^=, ]*}"
    grep -m1 '^overlay  *[^ ]*/top  *.*upperdir='"$overbase/mnt-install" /proc/mounts | { read dev mntdir fstype opts; echo $mntdir; }
}

mount_overlays() {
    local overbase=$1
    [ $EUID = 0 ] || { 
        echo "CANNOT CURRENTLY START CHROOT WITHOUT ROOT"
        exit -1
    }
    [ -z "$overbase" ] && { echo Cannot perform mount with no overbase path; exit -1; }
    mount_overlays_asroot $overbase
}

mount_overlays_asroot() {
    local overbase=$1
    local mntchk=$(get_active_mounts $overbase)
    mntchk="${mntchk%% *}"
    [ -z "$mntchk" -o \! -d "$mntchk" ] || { echo "ERROR: attempted second overlay with same upperdir="; exit -1; }

    [ $EUID = 0 ] || { echo "ERROR: only root can run this script."; exit -1; }
    grep -wq overlay /proc/filesystems ||
	modprobe overlay ||
	{ echo "ERROR: requires overlay filesystem (from overlay.ko kernel module)."; exit -1; }

    local mnt="$(mktemp -d)"
    mount -t tmpfs tmpfs $mnt || exit -1
    mnt="$mnt/top"
    mkdir -p $mnt
    declare -g mnttop=$mnt

    mntover=${mnt##*/}
    mntupper=$overbase/mnt-install/$mntover
    mntwork=$overbase/mnt-work/$mntover

    rm -rf $overbase/mnt-install
    mkdir -p $mntupper $mntwork || return

    [ -d $mnt -a -d $mntupper -a -d $mntwork ] || return;

    mkdir -p $mntupper@ $mntwork@
    mount -t overlay overlay -o "defaults,upperdir=$mntupper@,lowerdir=/,workdir=$mntwork@" $mnt || exit -1

    # clear tmp and make safe..
    mount -t tmpfs tmpfs $mnt/tmp || exit -1

    # need to avoid a loop with an overlay mounting its own upper dir
    loopmntfs="$(stat -c %m ${mntupper}@)"
    [ "$loopmntfs" != "/" ] || { echo "must run chroot-install in dir (${mntupper}@) outside root filesystem"; exit -1; }

    if [ $loopmntfs = /opt ]; then
    	for i in /opt/*/.; do
	    # ignore non-dirs
	    [ -d $i -a ! -L $i ] || continue;
	    # ignore non-storing dirs
	    case $i in
	    	/opt/zmanda/.) ;;
	    	/opt/lost+found/.) ;;
		*)  mkdir -p $mnt$i; ( set -x; mount --bind $i $mnt$i; );
	    esac
	done
    else
	( set -x; mount -o remount,defaults,bind,ro $loopmntfs $mnt$loopmntfs; )
    fi

    sort -u -k2,2 -b /proc/mounts | 
    while read dev mntdir type opts; do
	[ $type = "autofs" ] && continue  # cannot handle these in the chroot
	[ $type = "devpts" ] && continue  # leave these for binding
	[ $mntdir = "/run" ] && continue  # leave these for binding

	[ "${mntdir#$mnt}" != "$mntdir" ] && continue # don't re-process our own mounts
        mntdir=${mntdir%/}
        [ -z "$mntdir" ] && continue  # dont do root here
	[ ! -d $mntdir -o ! -d $mnt$mntdir ] && continue  # skip if not a mntdir to mount
        [ "$(stat -c %m $mnt$mntdir)" = $mnt$mntdir ] && continue  # skip if mounted

	[ ${dev#/dev/} != $dev ] ||
        [ ${dev#UUID=} != $dev ] ||
        {
            ( set -x; mount -t ${type} ${type} $mnt$mntdir; ) && 
               echo "(${mnt#/tmp/tmp.}) mount (-t $type) okay $mntdir" ||
               echo "(${mnt#/tmp/tmp.}) mount (-t $type) failed $mntdir"
	    continue
        }

	mkdir -p $mntupper$mntdir@ $mntwork$mntdir@
	( set -x; mount -t overlay overlay -o "defaults,upperdir=$mntupper$mntdir@,lowerdir=$mntdir,workdir=$mntwork$mntdir@" $mnt$mntdir; )  &&
               echo "(${mnt#/tmp/tmp.}) overlay mount okay $mntdir" ||
               echo "(${mnt#/tmp/tmp.}) overlay mount failed $mntdir"
        true
    done || exit -1

    # need the live sockets and other things to run...
    mount --bind /run $mnt/run
    mount --bind /dev/pts $mnt/dev/pts

    #
    # entire path needed for install.. (if /opt exists or not)
    #
    optmntdir="/opt/zmanda"
    emptydir="${mnttop%/*}/empty"
    mkdir -p $mntupper$optmntdir@ $mntwork$optmntdir@ $mnt$optmntdir $emptydir
    [ -d $emptydir -a -d $mnt$optmntdir ] || exit -1
    ( set -x; mount -t overlay overlay -o "defaults,upperdir=$mntupper$optmntdir@,workdir=$mntwork$optmntdir@,lowerdir=$emptydir" $mnt$optmntdir; ) || exit -1
    echo "(${mnt#/tmp/tmp.}) overlay mount okay $optmntdir"
    
    #
    # protect build tree (if present near overlay-storage) to allow builds/installcheck
    #
    if [ -d ./rpmbuild ]; then
        curdir="$(realpath .)/rpmbuild"
    elif [ -d ./debbuild ]; then
        curdir="$(realpath .)/debbuild"
    else
        curdir=
    fi

    #
    # protect build tree (if present near overlay-storage) to allow builds/installcheck
    #
    if [ -n "$curdir" ]; then
        mkdir -p $mntupper$curdir@ $mntwork$curdir@
        ( set -x; mount -t overlay overlay -o "defaults,upperdir=$mntupper$curdir@,workdir=$mntwork$curdir@,lowerdir=$curdir" $mnt$curdir; ) || exit -1
        echo "(${mnt#/tmp/tmp.}) overlay mount okay $curdir"
    fi

    grep " $(stat -c %m $mnt/etc) " /proc/mounts | grep -w overlay || echo "WARNING: $mnt/etc is not overlay protected" >&2
    grep " $(stat -c %m $mnt/usr) " /proc/mounts | grep -w overlay || echo "WARNING: $mnt/usr is not overlay protected" >&2
    grep " $(stat -c %m $mnt/var) " /proc/mounts | grep -w overlay || echo "WARNING: $mnt/var is not overlay protected" >&2
}

umount_all_overlays() {
    # only used at end if needed...
    local overbase=$1

    local mnt="$(get_active_mounts "$overbase")"
    mnt="$(realpath -e $mnt 2>/dev/null)" || return
    local mntover=${mnt##*/}

    local mntchk=${mnt#/tmp/tmp.??????????/}
 
    [ -d $mnt -a $mnt != $mntover -a $mntchk = $mntover ] || return;

    ( set -x;
	[ -x "$mnt/etc/init.d/zmc_aee" ] && 
	    chroot $mnt /etc/init.d/zmc_aee stop >/dev/null 2>&1
    )

    # dangerous ... but should be filtered okay..
    find /proc -maxdepth 3 -name root -lname $mnt | 
       while read i; do 
           [ -d $i ] || continue
           i=${i#/proc/}  
           i=${i%/root}
           set -x
           [ "$i" -gt 0 ] && ps $i 
           [ "$i" -gt 0 ] && kill -9 $i
	   sleep 1
           set +xv
       done

    sleep 1

    sort -ru -k2,2 -b /proc/mounts | 
    while read dev mntdir type opts; do 
        [ $mntdir != "${mntdir#$mnt}" ] || continue;
        mntdir="${mntdir#$mnt}"
        [ -z "$mntdir" ] && continue

        [ -d "$mnt$mntdir" ] && umount -l -R "$mnt$mntdir" && echo "(${mnt#/tmp/tmp.}) umount okay $mntdir"
        [ -d "$mnt$mntdir" ] && ( cd $mnt; rmdir -p ./$mntdir >/dev/null 2>&1; )
    done;

    [ -d $mnt ] && 
       { umount -l -R $mnt && echo "(${mnt#/tmp/tmp.}) umount okay / <topmost>"; } || 
       { echo "failed to umount top"; exit -1; }

    mnttopfs="$(stat -c %m $mnt)"
    [ $mnt != $mnttopfs ] || exit -1;

    # PREVENT ANY UMOUNT OR DELETION UNLESS UNDER /tmp/...!
    if [ $mnttopfs != "${mnttopfs#/tmp/}" ]; then
        [ -d $mnttopfs ] && 
           { umount -R -l $mnttopfs && echo "(${mnt#/tmp/tmp.}) umount okay <tmpfs>"; } || 
           { echo "failed to umount tmpfs"; exit -1; }
        rmdir $mnttopfs
    fi

    # wait .. this should be gone!  (at least the lowest part)
    [ ! -d $mnt ] || exit -1

    mntupper=$overbase/mnt-install/$mntover
    mntwork=$overbase/mnt-work/$mntover
    if [ -n "$overbase" ] && [ -d $mntupper -o -d $mntwork ]; then
	    [ -d $mntupper ] && find $mntupper -type c -perm 000 -delete 
	    [ -d $mntupper/root ] && rm -f $mntupper/root/.bash_history
	    [ -d $mntwork ] && rm -rf $mntwork
    fi
}


chroot_install() 
{
    mnttop=$1
    shift
    files=("$@")

    [ -n "$mnttop" -a -d "$mnttop" ] || { echo "ERROR: cannot chroot to $mnttop"; exit -1; }

    [ $EUID = 0 ] || { echo "ERROR: only root can run this script."; exit -1; }

    pidof rpm && { echo "ERROR: external rpm can not be running during install test."; exit -1; } || true
    pidof dpkg && { echo "ERROR: external dpkg can not be running during install test."; exit -1; } || true
    pidof apt-get && { echo "ERROR: external apt-get can not be running during install test."; exit -1; } || true
    pidof yum && { echo "ERROR: external yum can not be running during install test."; exit -1; } || true
    pidof zypper && { echo "ERROR: external zypper can not be running during install test."; exit -1; } || true

    export CWD=$(realpath ${PWD})

    # set of array indices
    for i in ${!files[@]}; do 
        f=${files[$i]};

        # skip options
	[[ "$f" == -?* ]] && continue;

	f=$(realpath "$f");
	f="${f#${CWD}/}"
	[[ "$f" == [^/]* ]] && f="./$f"

        files[$i]="$f"
        [ "${f%.rpm}" != $f ] && { 
	    rpm -q -p $f || { echo "cannot read $f rpm file"; exit -1; }; 
	    name=$(rpm -q --queryformat='%{NAME}' -p $f)
	    chroot $mnttop /bin/bash -c "cd $CWD; rpm -evh --nodeps $name 2>/dev/null 2>&1; true" || exit -1;
	}
        [ "${f%.deb}" != $f ] && { 
	    dpkg-deb -W $f || { echo "cannot read $f rpm file"; exit -1; }; 
	    name="$(dpkg-deb -W $f)"
	    name="${name%%[ 	]*}"
	    chroot $mnttop /bin/bash -c "cd $CWD; dpkg -r $name 2>/dev/null 2>&1; true" || exit -1
	}
        [ "${f%.run}" != $f ] && {
	    # TODO: no uninstall comes to mind yet...
	    chmod a+x $f || { echo "cannot read $f rpm file"; exit -1; }; 
        }
    done

    systemctl stop xinetd
    [[ "$(systemctl show -p SubState xinetd)" == *=dead ]] || 
	{ echo "cannot halt xinetd as user $(id -u -n)"; exit -1; }; 

    [ "${f%.rpm}" != $f ] && { 
	if command -v yum >/dev/null; then
	    yum makecache
	    chroot $mnttop /bin/bash -xc \
		    'umask 022; cd $CWD; SYSTEMCTL_SKIP_REDIRECT=1 _SYSTEMCTL_SKIP_REDIRECT=1 yum install -y "$@";' \
		    -- "${files[@]}" || 
		{ systemctl start xinetd; exit -1;  }
	elif command -v zypper >/dev/null; then
	    zypper -D update; 
	    chroot $mnttop /bin/bash -xc \
		    'umask 022; cd $CWD; SYSTEMCTL_SKIP_REDIRECT=1 _SYSTEMCTL_SKIP_REDIRECT=1 zypper install --allow-unsigned-rpm -y "$@";' \
		    -- "${files[@]}" || 
		{ systemctl start xinetd; exit -1;  }
	fi
    }

    [ "${f%.deb}" != $f ] && { 
        apt-get update
	chroot $mnttop /bin/bash -xc \
		'umask 022; echo "installing files list:"; cd $CWD; ls -1 "$@"; SYSTEMCTL_SKIP_REDIRECT=1 _SYSTEMCTL_SKIP_REDIRECT=1 DEBIAN_FRONTEND=noninteractive apt-get install -y "$@";' -- "${files[@]}" || 
	    { systemctl start xinetd; exit -1;  }
    }
    [ "${f%.run}" != $f ] && { 
	chroot $mnttop /bin/bash -xc \
		'umask 022; echo "installing files list:"; cd $CWD; ls -1 "$1"; SYSTEMCTL_SKIP_REDIRECT=1 _SYSTEMCTL_SKIP_REDIRECT=1 "$@";' \
		-- "${files[@]}" || 
	    { systemctl start xinetd; exit -1;  }
    }

    # needed for some tests...
    sed -i -e '/restore_by_amanda_user=no/s/.*/restore_by_amanda_user=yes/' $mnttop/etc/amanda-security.conf

    # must not continue until any backgrounders are done
    for i in {0..2}{0..9}; do 
        pidof rpm && { sleep 1; continue; }
        pidof dpkg && { sleep 1; continue; }
        break;
    done

    systemctl start xinetd

    unset CWD
    true
}


chroot_cwd_owner_cmd() {
    mnttop=$1
    shift
    [ -n "$mnttop" -a -d "$mnttop" ] || { echo "ERROR: cannot chroot to $mnttop"; exit -1; }

    export LOGNAME=$(stat -c %U .)

#    echo "chroot $mnttop as $LOGNAME"

    CWD="${PWD}" chroot $mnttop \
		    /bin/su $LOGNAME -s /bin/bash -c \
			'umask 022; cd "$CWD"; eval "$@";' \
		-- bash "$@" || exit -1
}

chroot_amandabackup_cmd() {
    local overbase=$1
    mnttop="$(get_active_mounts $overbase)"
    shift
    [ -n "$mnttop" -a -d "$mnttop" ] || { echo "ERROR: cannot chroot to $mnttop"; exit -1; }
    [ "$#" -gt 0 ] || { echo "ERROR: cannot chroot to $mnttop"; exit -1; }

    # echo "chroot $mnttop as amandabackup"
    CWD="${PWD}" chroot $mnttop \
		    /bin/su amandabackup -s /bin/bash -c \
			'umask 022; cd "$CWD"; eval "$@";' \
		-- bash "$@" || exit -1
}

sysd_start_container() {
    local name=$1
    local mnt
    local mnttop
    local cmd=()

    [[ $name == */* ]] && {
        echo "Name cannot have directories / slashes"
        exit -1
    }

    [ $EUID = 0 ] || { 
        echo "CANNOT CURRENTLY START CHROOT WITHOUT ROOT"
        exit -1
    }

    command -v systemd-nspawn >/dev/null || {
        echo "CANNOT USE SYS-MOUNT-OVERLAYS WITHOUT systemd-nspawn"
        exit -1
    }

    [[ "$(systemd-nspawn --version)" == systemd\ 2[34][0-9]* ]] || { 
        echo "CANNOT USE systemd-nspawn VERSION < 2.3X or 2.4X"
        exit -1
    }

    read avail mnt fsmnt < <(df -BK --output=avail,file,target /opt/tmp /pkgs/tmp /home/test/opt/tmp | sort -rbh | head -1)
    # need some real storage space if possible
    mnttop="$(mktemp -d -p $mnt $name-overlay-XXXX)" 
    fsmnttop=$(stat -c %m $mnttop)
    mnt=${mnttop}.root
    mkdir ${mnt}
    chmod a+rwX,o+t ${mnttop}

    [ -d "$mnt" -a -w $mnt ] || exit 1
    [ -d "$fsmnt" ] || exit 1

    # overlay=+/.. means relative to image-path (same in this case)
    # overlay=.. ::.. means use a temporary directory from /var/tmp that is deleted later
    # overlay=/dir means the dir itself

    readarray -t cmd < <(
        local type
        local dev
        local mntdir
	local cmd=()

	# must have RO basics to start at all
	mount -o ro --bind / ${mnt}

	# fedora can populate the /var dirs
        cmd=(--volatile=state -D${mnt})
	# keep from conflicting for now...
        cmd+=(--link-journal=no)
	
        exec < <(sort -bu -k2,2 /proc/mounts)
        while read dev mntdir type opts; do
            [ -z $mntdir ] && continue
            [ $mntdir = / ] && continue
            [[ $mntdir = ${mnt}* ]] && continue

	    # skip mnt and mnttop if they show up...
            [ $mntdir -ef ${mnt} ] && continue
            [ $mntdir -ef ${mnttop} ] && continue

	    # must skip /var
            [ $mntdir -ef /var ] && continue
         
             [[ $dev == /dev/* ]] || 
             [[ $dev == UUID=* ]] ||
                 continue
        
            [ -d ${mnt}$mntdir ] || 
		[ -n "${mntdir%/*}" ] || {
       		echo "warning: NO MOUNT POINT for $mntdir" 
	    	continue
	    }

            # tmpfs and all others skips here
            cmd+=("--bind-ro=$mntdir")
        done 

        cmd+=("--tmpfs=${mnttop%%/tmp/*}/tmp")      # cover up the ro-tmp where overlays are

	for path in /etc /usr /opt/zmanda /var/lib /var/log ${CWD}; do
	    dir="${mnttop}/${path//\//_}/overlay"
	    mkdir -p $dir
	    cmd+=("--overlay=${path}:${dir}:${path}")  # use a random /var/tmp store
	done

        IFS=$'\n'
        echo "${cmd[*]}"
    )

    sysd_shutdown_machine $name

    ( systemd-nspawn -M $name -b "${cmd[@]}" -- --unit=basic.target >& tee $mnt.$$.log & )

    if ! sysd_wait_machine_present; then
	echo "WARNING: failed to start: $mname $rest"; 
	return 1; 
    fi

    echo "started: $mname $rest"
    return 0
}

sysd_shutdown_machine() {
    local name=$1
    local mname
    local class
    local svc
    local rest

    machinectl poweroff $name >&/dev/null
    for i in {1..20}; do 
        read mname class svc rest <<<"$( machinectl list --no-legend --no-pager -l | grep "^$name" )"
        [ $class = container -a $svc = systemd-nspawn -a "$mname" = "$name" ] || break
        sleep 0.2
    done
}

sysd_wait_machine_present() {
    local mname
    local class
    local svc
    local rest
    for i in {1..20}; do 
       read mname class svc rest <<<"$( machinectl list --no-legend --no-pager -l | grep "^$name" )"
       [ "$class" = container -a "$svc" = systemd-nspawn -a "$mname" = "$name" ] && break
       sleep 0.2
    done
    [ "$mname" = $name ]
}


sysd_wait_target_reached() {
    local name=$1
    for i in {0..120}; do
	sysd_wait_machine_present "$name" || return 1
	# Mar 04 23:44:34 os-fedora30.local systemd[1]: Reached target Basic System.
	log="$(systemd-run --wait -P -M $name /usr/bin/journalctl -b -0 -u basic.target 2>&1)"
	grep <<<"$log" -iq 'Reached target Basic System' && return 0
	grep <<<"$log" -iq 'Host is down' && return 1
	echo -n 'waiting +$i: '; tail -1 <<<"$log"
	sleep 0.5
    done
    return 1
}

sysd_stop_all_containers() {
    local mname
    local class
    local svc
    local rest

    # MACHINE CLASS     SERVICE        OS VERSION ADDRESSES
    # testing container systemd-nspawn - - -
    machinectl list --no-legend --no-pager -l | 
       while read mname class svc os vers addr; do 
           [ $class = container -a $svc = systemd-nspawn ] && machinectl poweroff $mname
       done

    for i in {1..20}; do 
	out="$( machinectl list --no-legend --no-pager -l)"

	cnt=0
	while read mname class svc rest; do 
	    [ "$class" = container -a "$svc" = systemd-nspawn -a "$mname" = "$name" ] && : $(( ++cnt ))
	done <<<"$out"

	[ $cnt = 0 ] && return 0
	echo 'waiting for shutdowns $cnt +$i...';
        sleep 0.2
    done
    return 1
}

sysd_cwd_cmd() {
    local name=$1
    shift
    local cmd="$1"
    shift

#    echo "sysd $mnttop as $LOGNAME"
    systemd-run --wait -t -M $name --uid=$UID /bin/bash -xc "$cmd" -- "$@" || 
       exit -1
}

sysd_cwd_owner_cmd() {
    local name=$1
    shift
    local cmd="$1"
    shift

    uid=$(stat -c %u .)

#    echo "sysd $mnttop as $LOGNAME"
    systemd-run --wait -t -M $name --uid=$uid /bin/bash -xc "$cmd" -- "$@" || 
       exit -1
}

sysd_amandabackup_cmd() {
    local name=$1
    shift
    local cmd="$1"
    shift

    uid=$(id -u amandabackup)
    systemd-run --wait -t -M $name --uid=$uid /bin/bash -c "$cmd" -- "$@" || 
       exit -1
}

sysd_install() {
    local mname=$1
    shift
    files=("$@")

    [ $EUID = 0 ] || { echo "ERROR: only root can run this script."; exit -1; }

    export CWD=$(realpath ${PWD})

    # loop of array indices
    for i in ${!files[@]}; do 
        f=${files[$i]};

        # skip options
	[[ "$f" == -?* ]] && continue;

	f=$(realpath "$f");
	f="${f#${CWD}/}"
	[[ "$f" == [^/]* ]] && f="./$f"

        files[$i]="$f"
	case $f in 
	    *.rpm) rpm -q -p $f || { echo "cannot read $f rpm file"; exit -1; }; 
		pkg=$(rpm -q --queryformat='%{NAME}' -p $f)
		sysd_cwd_cmd $mname "rpm -evh --nodeps $pkg 2>/dev/null 2>&1; true" || exit -1;
    	    ;;
    	    *.deb) dpkg-deb -W $f || { echo "cannot read $f rpm file"; exit -1; }; 
		pkg="$(dpkg-deb -W $f)"
		pkg="${name%%[ 	]*}"
		sysd_cwd_cmd $mname "dpkg -r $pkg 2>/dev/null 2>&1; true" || exit -1
	    ;;
    	    *.run)
		#sysd_cwd_cmd $mname "${TOPDIR}/zmanda-backup-pkg/uninstall--mode uninstall"
		#sysd_cwd_cmd $mname "${TOPDIR}/zmanda-zmc-pkg/uninstall --mode uninstall"
		chmod a+x $f || { echo "cannot read $f rpm file"; exit -1; }; 
		sysd_cwd_cmd $mname "cd $CWD; umask 077; \"\$@\";" "$f" --mode unattended
	    ;;
	esac
    done

    # must do these all at once...
    list=($(IFS=$'\n'; grep <<<"${files[*]}" '\.rpm$'))
    if [ ${#list[@]} -gt 0 ]; then
	if command -v yum >/dev/null; then
	    yum makecache
	    sysd_cwd_cmd $mname 'yum install -y "$@";' -- "${list[@]}" ||
		exit -1;
	elif command -v zypper >/dev/null; then
	    zypper -D update; 
	    sysd_cwd_cmd $mname 'zypper install --allow-unsigned-rpm -y "$@";' "${list[@]}" || 
		exit -1;
	fi
    fi

    list=($(IFS=$'\n'; grep <<<"${files[*]}" '\.deb$'))
    if [ ${#list[@]} -gt 0 ]; then
	apt-get update
	sysd_cwd_cmd $mname 'DEBIAN_FRONTEND=noninteractive apt-get install -y "$@";' "${list[@]}" ||
	    exit -1;
    fi

    # needed for some tests...
    sysd_cwd_cmd $mname "sed -i -e '/restore_by_amanda_user=no/s/.*/restore_by_amanda_user=yes/' /etc/amanda-security.conf || true"

    # must not continue until any backgrounders are done
    for i in {0..2}{0..9}; do 
        pidof rpm && { sleep 1; continue; }
        pidof dpkg && { sleep 1; continue; }
        break;
    done

    unset CWD
    true
}

zmrecover_test=$(cat <<'CLIENT_STATUS'
<QUERY>
CLIENT-STATUS
amclient_timelimit={$job['amclient_timelimit']}
config={$job['config']}
debug=$debug
digest={$job['digest']}
disk_device={$job['disk_device']}
disk_name={$job['disk_name']}
dryrun={$job['dryrun']}
history={$historyFileName}
job_name={$job['job_name']}
password=root
safe_mode=".(empty($job['safe_mode']) ? 'off':'on')
user_id=$user_id
username=$username
zmc_build_branch=" . ZMC::$registry->svn->zmc_build_branch
zmc_build_version=" . ZMC::$registry->svn->zmc_build_version
zmc_svn_build_date=" . ZMC::$registry->svn->zmc_svn_build_date
zmc_svn_distro_type=" . ZMC::$registry->svn->zmc_svn_distro_type 
zmc_svn_revision=" . ZMC::$registry->svn->zmc_svn_revision
</QUERY>
CLIENT_STATUS
)

