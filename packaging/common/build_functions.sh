#!/usr/bin/env bash
# Common Functions

# Required variables:
# LOGFILE
# SYSCONFDIR
# os

rpmbuild_opts='--rpmfcdebug '

unset CDPATH
unset git
setopt=$-
set +xv
set +o posix

die() {
   echo "$@" 1>&2
   exit 255
}

# dont allow aliases for these
xargs() { command xargs -r "$@"; }
realpath() { command readlink -e "$@"; }

if command -v gtar >/dev/null; then
   tar() { command gtar "$@"; }
else
   tar() { command tar "$@"; }
fi

if command -v gmake >/dev/null; then
   make() { command gmake "$@"; }
   export MAKE=$(command -v gmake)
else
   make() { command make "$@"; }
   export MAKE=$(command -v make)
fi

# needed for latest non-sun solaris ssh
[ -x /opt/csw/bin/ssh ] &&
   export GIT_SSH=/opt/csw/bin/ssh

git() { command git "$@"; }

# find abs top-dir path
[ -d "$src_root" ]      || src_root="$(realpath .)"
[[ "$src_root" == /* ]] || src_root="$(realpath .)"

pkgdirs_top=${pkgdirs_top:-$(realpath $src_root/packaging/.)}


# normally $src_root/packaging/.
#    - pkgdirs_top
#
# normally $src_root/packaging/rpm
# normally $src_root/packaging/deb
# normally $src_root/packaging/whl
# normally $src_root/packaging/sun-pkg
#    - buildpkg_dir

# determined by build machine, OS and architecture
#
# normally .rhel7.x86_64.rpm
# normally -1Ubuntu1604_amd64.deb
#    - pkg_suffix
#
# normally rpm
# normally whl
# normally deb
# normally sun-pkg
#    - pkg_type

get_yearly_tag() {
    local t=$(( ${1:-$pkg_name_pkgtime} + 0 ))

    # use external pkg_name_pkgtime
    t=${t#0}
    t=${t:-$(TZ=GMT date +%s)}

    local yr=$(TZ=GMT date -d "@$t" +%Y)
    local top_of_year=$(TZ=GMT date -d "Jan 1 $yr" +%s)

    echo -n "$(( yr % 100 ))+"
    # change base64 to match ASCII in ordering
    gawk 'BEGIN {
        n=int( (ARGV[1] - ARGV[2])/70 );
        all = "AEIOUbcdfghjklmnpqrstvwxyz";  # w/caps ascii ordering..
        s = substr(all,(n % 26),1); n = int(n/26);
        s = substr(all,(n % 26),1) s; n = int(n/26);
        s = substr(all,(n % 26),1) s; n = int(n/26);
        s = substr(all,(n % 26),1) s; n = int(n/26);
        printf("%s\n",s)
    }'  $t $top_of_year
}


#
# a horrible function that uses the src_root to try and find the packaging context
#
# or tries to guess the packaging context by seeing the directory of the highest
# calling bash script.
#
detect_pkgdirs_top() {
    local calldepth=$(( ${#BASH_SOURCE[@]} - 1 ))
    local topcall=${BASH_SOURCE[${calldepth}]}
    local pkgsdirs_toptest=
    local d=

    # try a smart default for a standard autochk dir
    declare -g pkgdirs_top=$src_root/packaging

    topcall="$(realpath -e "${topcall}" || echo $0)"

    # not a real script path?
    [ -f "$topcall" ] ||
        return 1

    local topcall_dir="${topcall%/*}"
    declare -g buildpkg_dir="${buildpkg_dir:-${topcall_dir}}"

    d=${buildpkg_dir}

    # script is real but not a clear directory location
    # top common directory is based on packaging repo common dir ... 
    # correct pkgdirs_top is just below it
    [ ! -L $buildpkg_dir/../common -a -d $buildpkg_dir/../common ] && d=$buildpkg_dir/..
    [ ! -L $buildpkg_dir/../../common -a -d $buildpkg_dir/../../common ] && d=$buildpkg_dir/../..
    [ ! -L $buildpkg_dir/../../../common -a -d $buildpkg_dir/../../../common ] && d=$buildpkg_dir/../../..

    # if called from within common or scripts ... just use "packaging" as top
    [ $d -ef $d/../common ] && d+=/..
    [ $d -ef $d/../scripts ] && d+=/..

    [ -e "$d/.git" ] && d="${d%/..}" # take back one level to keep project directory...

    # narrow choices with calling script.s path, if possible
    declare -g pkgdirs_top="$(realpath -e $d)"
    local n=$(( ${#pkgdirs_top} ))

    # confirm dir if needed to assert pkg_type 
    case "${buildpkg_dir:$n}/" in
       # in case buildpkg dir is subdir of type (debian or sun)
       rpm/*|deb/*|sun-pkg/*|whl/*|tar/*) 
            pkg_type=${buildpkg_dir:$n}/
            pkg_type=${pkg_type%%/*}
        ;;
       # in case buildpkg dir is above type dir
       */rpm/|*/deb/|*/sun-pkg/|*/whl/|*/tar/)
            pkg_type=${buildpkg_dir:$n}
            pkg_type=${pkg_type%/}
            pkg_type=${pkg_type##*/}
        ;;
       (*) ;;  # leave as it was
    esac

    pkg_type=${pkg_type#/}
    pkg_type=${pkg_type%/}
    pkg_typedir=${pkg_type/#pkg/sun-pkg}

    # don't have any valid pkg_type??   must give up for here

    [ -z "$pkg_type" ] && return 1;
    [ ${pkgdirs_top}/${pkg_typedir} -ef ${pkgdirs_top}/common ] && return 1
    [ ${pkgdirs_top}/${pkg_typedir} -ef ${pkgdirs_top}/scripts ] && return 1

    # if script came from below the base pkgdirs_top ...
    [[ ${buildpkg_dir} = $pkgdirs_top/${pkg_typedir} ]] || 
       return 1
    [ -d ${buildpkg_dir} ] || 
       return 1

    declare -g pkg_type=${pkg_type}
    declare -g pkg_typedir=${pkg_typedir}
}

detect_build_dirs() {
    local presets
    local path_buildpkg_dir="$(readlink -e $buildpkg_dir)"

    # dont re-discover if already set
    declare >&/dev/null -p \
	 pkg_name \
	 pkg_type \
	 pkgconf_dir \
	 buildpkg_dir \
	 pkg_bldroot \
	 repo_name && 
      return;   # already have it all known!

    [ -n "$pkg_type" ] || die "cannot determine package type to use upon build"

    presets="declare -g pkg_type=\"$pkg_type\";"
    [ -n "$pkg_name" ] && presets+="declare -g pkg_name=\"$pkg_name\";"
    # [ -n "$repo_name" ] && presets+="declare -g repo_name=\"$repo_name\";"

    [ -n "$buildpkg_dir" ] ||
        declare -g buildpkg_dir=${pkgdirs_top}/${pkg_typedir}

    declare -g pkg_bldroot="$src_root/${pkg_type}build"
    case $pkg_type in
       (rpm) declare -g pkgconf_dir=SPECS ;;
       (deb) declare -g pkgconf_dir=debian ;;
       (pkg|sun-pkg)
	     declare -g pkg_bldroot=${pkg_bldroot/sun-pkg/pkg}
             declare -g pkgconf_dir=PKGBUILD
             buildpkg_dir="${buildpkg_dir%/pkg}"
             buildpkg_dir="${buildpkg_dir%/sun-pkg}"
             buildpkg_dir+="/sun-pkg"  # must add good suffix
             declare -g buildpkg_dir="${buildpkg_dir}"
             pkg_type=sun-pkg
            ;;
       (whl) declare -g pkgconf_dir=. ;;
       (tar) declare -g pkgconf_dir=BUILD ;;
       (*) die "cannot find pkg_type in time to set up build configs" ;;
    esac

    [[ $buildpkg_dir == $pkgdirs_top/$pkg_type ]] || 
       die "mismatch of $buildpkg_dir and $pkgdirs_top/$pkg_type top directory"

    # gather vars for current settings
    [ -r $buildpkg_dir/../../0_vars.sh ]    && { . $buildpkg_dir/../../0_vars.sh; }
    [ -r $buildpkg_dir/../0_vars.sh ]       && { . $buildpkg_dir/../0_vars.sh; }
    [ -r $buildpkg_dir/0_vars.sh ]          && { . $buildpkg_dir/0_vars.sh; }

    declare -g pkg_name=${pkg_name}
    # declare -g repo_name=${repo_name}
    declare -g pkg_type=${pkg_type}

    [ -d $buildpkg_dir ] || { unset buildpkg_dir; set +a; return 0; }

    declare -g pkg_name=${pkg_name}
    # declare -g repo_name=${repo_name}
    declare -g pkg_type=${pkg_type}

    # override if they had values originally
    eval "$presets"
}

detect_root_pkgtime() {
    local a=0
    local b=0
    local t="$(date +%s)"
    local now=$t
    local src_root_t=$t
    local buildpkg_dir_t=$t
    local pkg_common_t=$t
    local src_root_hash=build-time
    local buildpkg_dir_hash=build-time
    local pkg_common_hash=build-time
    local d

    git rev-parse --git-dir 2>/dev/null | grep -q . || return 0

    # if no differences..
    if git $git_srcroot_args diff --quiet --ignore-submodules=all $src_root; then
        src_root_hash=$(git $git_srcroot_args log --pretty='%h' -1 $src_root)
        src_root_t=$(git $git_srcroot_args log --pretty='%at' -1 $src_root)
        src_root_t=$(( src_root_t + 0 ))
    fi

    d=$(realpath $buildpkg_dir)
    if ! [ -n "$buildpkg_dir" -a -d "$d" ]; then
        buildpkg_dir_t=950000000  # feb 2000
        buildpkg_dir_hash=unkn
    # if no differences..
    elif git $git_pkgdirs_args diff --quiet --ignore-submodules=all $d; then
        buildpkg_dir_hash=$(git $git_pkgdirs_args log --pretty='%h' -1 $d)
        buildpkg_dir_t=$(git $git_pkgdirs_args log --pretty='%at' -1 $d)
        buildpkg_dir_t=$(( buildpkg_dir_t + 0 ))
    fi

    d=$(realpath "$pkgdirs_top/common/.")
    if ! [ -n "$pkgdirs_top" -a -d "$d" ]; then
        pkg_common_t=950000000  # feb 2000
        pkg_common_hash=unkn
    # if no differences..
    elif git $git_pkgdirs_args diff --quiet --ignore-submodules=all $d; then
        pkg_common_hash=$(git $git_pkgdirs_args log --pretty='%h' -1 $d)
        pkg_common_t=$(git $git_pkgdirs_args log --pretty='%at' -1 $d)
        pkg_common_t=$(( pkg_common_t + 0 ))
    fi

    {
    printf -- "#--------------- %-14s: %s @%s =%ds old \n" top-dir $(get_yearly_tag $src_root_t) $src_root_hash $(( src_root_t - now ))
    printf -- "#--------------- %-14s: %s @%s =%ds old \n" pkg-scripts $(get_yearly_tag $buildpkg_dir_t) $buildpkg_dir_hash $(( buildpkg_dir_t - now ))
    printf -- "#--------------- %-14s: %s @%s =%ds old \n" pkg-common $(get_yearly_tag $pkg_common_t) $pkg_common_hash $(( pkg_common_t - now ))
    } | LANG=C sort -t: -b -k2

    t=${src_root_t}
    [ $buildpkg_dir_t -gt $t ] && t=$buildpkg_dir_t
    [ $pkg_common_t -gt $t ] && t=$pkg_common_t

    declare -g pkg_name_pkgtime=$t
}

get_version_evalstr() {
    local f=$(realpath ${BASH_SOURCE[0]})
    ${BASH} -${setopt} ${f%/*}/version_setup.sh $1
}

logger() {
    local msg

	# A non-annoying way to log stuff
	# ${@} is all the parameters, also known as the message.  Quoting the input
	# preserves whitespace.
	msg="$(date +'%b %d %Y %T'): ${@}"
	echo "${msg}" >> ${LOGFILE}
}

log_output_of() {
    local output
    local ret

    # A non-annoying way to log output of commands
    # ${@} is all the parameters supplied to the function.  just execute it,
    # and capture the output in a variable.  then log that.
    output=`"${@}" 2>&1`
    ret=$?
    if [ -n "${output}" ] ; then
            logger "${1}: ${output}"
    fi
    return ${ret}
}

do_file_subst() {
    local file
    local target

    # get dir-name-above-script
    # Do substitutions..
    for file; do
	target="${file%.src}"
	[ -r "${file}" ] || die "substitution source file not found: $file"
	[ "$file" != "$target" ] ||
           die "substitution \"$file\" was not intended for substitution [${target}]";

        (
            exec <$file >$target
            cd $src_root;
	    export pkg_name=${pkg_name};
            export pkg_type=${pkg_type};
            $pkgdirs_top/common/substitute.pl /dev/stdin /dev/stdout;
        ) || { echo "substitution of \"$file\" -> \"$target\" failed somehow"; exit 255; }
	echo "substitution of \"$file\" -> \"$target\"" >&2
    done
    return 0
}

get_version() {
    # requires FULL_VERSION is in place already
    declare -g VERSION=$(cd $src_root; $pkgdirs_top/common/substitute.pl <(echo %%VERSION%%) /dev/stdout);
    [ -n "$pkg_name" ] &&
       declare -g PKG_NAME_VER="$pkg_name-$VERSION"
}

gen_pkg_build_config() {
    local buildparm_dir=$1

    detect_package_vars  # in case it wasn't run

    buildparm_dir="${buildparm_dir:-${buildpkg_dir}}"  # in case it wasnt provided

    [ -d "$buildparm_dir" ] ||
	die "ERROR: gen_pkg_build_config() \"$buildparm_dir\" bad setup directory"
    [ "$buildpkg_dir/../$pkg_typedir" -ef "$buildpkg_dir" ] ||
	die "ERROR: gen_pkg_build_config() invoked by script [$0] --> [$buildpkg_dir] outside of $pkg_type dir"

    buildparm_dir="$(realpath -e "$buildparm_dir")"

    # Check for the packaging dirs.
    if [ -z "$PKG_DIR" ]; then
	export PKG_DIR=$src_root
    fi
    if [ ! -d ${PKG_DIR} ]; then
	mkdir -p ${PKG_DIR} ||
	   die "top directory for build PKG_DIR=${PKG_DIR} cannot be created"
    fi
    # ---------------------------------------------------
    (
    cd ${PKG_DIR} || exit 100

    mkdir -p $pkg_bldroot
    cd "$pkg_bldroot" || exit 100

    case $pkg_bldroot in
	(*/rpmbuild)
	    echo "Config rpm package from $buildparm_dir =============================================="
	    mkdir -p {SOURCES,SRPMS,SPECS,BUILD,RPMS,BUILDROOT} ||
		   die "top directories for build under $(realpath .) cannot be created."
	    # Copy files into rpmbuild locations
            rm -rf $pkgconf_dir
	    cp -av $buildparm_dir $pkgconf_dir ||
		die "failed to copy spec files ($buildparm_dir/*.src) to $pkgconf_dir";
            # remove spurious vars files..
            rm -f $pkgconf_dir/[0-9]*
	    ;;

	(*/???build)
	    echo "Config $pkg_type package from $buildparm_dir ${buildpkg_dir:+and $buildpkg_dir }=============================================="
	    [ ! -e "$pkgconf_dir" ] || [ -d "$pkgconf_dir" ] || die "could not recreate $pkgconf_dir"
	    rm -rf $pkgconf_dir
            mkdir -p $pkgconf_dir
            # first the upper level ... then the pkg-specific files will overwrite
            find $buildpkg_dir/* -maxdepth 0 -type f -print |
              xargs -rl cp -fv -t $pkgconf_dir ||
                die "failed to copy all files from $buildpkg_dir to $pkgconf_dir"
            find $buildparm_dir/* -maxdepth 0 -type f -print |
              xargs -rl cp -fv -t $pkgconf_dir ||
                die "failed to copy all files from $buildparm_dir to $pkgconf_dir";

	    [ $pkg_type = deb -a ! -r $pkgconf_dir/control.src -a ! -r $pkgconf_dir/control ] &&
		die "$pkgconf_dir control (nor control.src) file was not present in $buildparm_dir nor selected automatically";
	    ;;

	 (*) die 'Unknown packaging for resources';;
    esac
    true
    ) || exit $?
    # ---------------------------------------------------
}

gen_top_environ() {
    # simulate the top directory as the build one...
    cd ${PKG_DIR}

    set_zmanda_version HEAD

    [ -n "${PKG_NAME_VER}" ] ||
        die "PKG_NAME_VER is not set correctly"

    [ -d $pkg_bldroot ] ||
	die "missing call to gen_pkg_build_config() or missing $pkg_bldroot directory"

    (
    cd $pkg_bldroot || exit 100

    case $pkg_bldroot in
	(*/rpmbuild)
            rm -rf BUILD/$PKG_NAME_VER
	    ln -sf $(realpath ..) BUILD/$PKG_NAME_VER || die "could not create symlink"
            # gzip -c < $VERSION_TAR > SOURCES/${PKG_NAME_VER}.tar.gz
	    ;;
	(*/???build)
	    # simulate the top directory as the build one...
            rm -rf $PKG_NAME_VER
	    ln -sf $(realpath ..) $PKG_NAME_VER || die "could not create symlink"
	    ;;
  	(*) die "Unknown packaging for resources in $pkg_bldroot"
	    ;;
    esac
    true
    ) || exit $?
}

gen_pkg_environ() {
    local tmp

    cd ${PKG_DIR}

    set_zmanda_version HEAD

    [ -n "${PKG_NAME_VER}" ] ||
        die "PKG_NAME_VER is not set correctly"

    tmp=$(mktemp -d)
    rm -f $tmp/${PKG_NAME_VER}
    ln -sf ${PKG_DIR} $tmp/${PKG_NAME_VER}

    [ -d $pkg_bldroot ] ||
    	die "missing call to gen_pkg_build_config() or missing $pkg_bldroot directory"
    (
    cd $pkg_bldroot || exit 100

    case $pkg_bldroot in
	(*/rpmbuild)
            rm -f SOURCES/${PKG_NAME_VER}.tar
	    [ -L BUILD/$PKG_NAME_VER ] && die "can not perform tar-copy via gen_pkg_version through $(realpath BUILD/${PKG_NAME_VER})"
	    tar -cf SOURCES/${PKG_NAME_VER}.tar \
		   --exclude=\*.rpm \
		   --exclude=\*.deb \
		   --exclude=.git \
		   --exclude=\*.tar.gz \
		   --exclude=\*.tar \
		   --exclude=\?\?\?build \
		    -C $tmp ${PKG_NAME_VER}/. ||
			die "tar creation from $(readlink ${PKG_NAME_VER} || echo "<missing symlink>") failed"
            #
	    # ready for the spec file to untar it
            gzip -f SOURCES/${PKG_NAME_VER}.tar
	    ;;
	(*/???build)
	    [ -L $PKG_NAME_VER ] && die "can not perform tar-copy via gen_pkg_version through $(realpath ${PKG_NAME_VER})"
	    rm -rf $PKG_NAME_VER
	    tar -cf - \
		   --exclude=\*.rpm \
		   --exclude=\*.deb \
		   --exclude=\*.pkg \
		   --exclude=\*.p5p \
		   --exclude=.git \
		   --exclude=\*.tar \
		   --exclude=\*.tar.gz \
		   --exclude=\?\?\?build \
		    -C $tmp $PKG_NAME_VER/. |
		tar -xf - ||
		    die "tar-based copy from $(readlink ${PKG_NAME_VER} || echo "<missing symlink>") to $pkg_bldroot failed"
            #
	    # ready for the build system to use it
            #
	    ;;
	 (*) die "Unknown packaging for resources in $pkg_bldroot"
	    ;;
    esac
    true
    ) || exit $?
    # ---------------------------------------------------

    rm -f $tmp/${PKG_NAME_VER}
}

#
# XXX: NEARLY THE SAME AS do_package ...
#
do_top_package() {
    local ctxt=$1
    local build_srcdir
    local hooks
    local repo_targz
    shift

    [ -d $pkg_bldroot ] ||
	die "missing call to gen_pkg_build_config() or missing $pkg_bldroot directory"

    (
    cd $pkg_bldroot

    case $pkg_bldroot in
	(*/rpmbuild)
            echo "Fake-Building via $ctxt =============================================="
            spec_file=$ctxt
            repo_targz=SOURCES/${PKG_NAME_VER}.tar.gz

            # dont need to untar subst-files...
            # NOTE: must be in the BUILD/xxx dir to subsitute anything correctly

            ( cd ..; find $pkg_bldroot/$pkgconf_dir/*.src | while read i; do do_file_subst $i; done; ) ||
                    die "missing call to gen_pkg_environ() or failed substitutions"
            rm -f $pkgconf_dir/*.src $pkgconf_dir/build*

            # move files to BUILD only when ready to...

            [ -s $pkgconf_dir/$spec_file ] ||
		die "ERROR: $pkgconf_dir/$spec_file is not present for packaging step"
            [ -n "$(command -v rpmbuild)" ] ||
		die "ERROR: rpmbuild command was not found.  Cannot build without package \"rpm-build\" installed."
	    [ -f $repo_targz ] ||
		die "ERROR: missing call to gen_top_environ()"

            # %setup lines are useless for top-build.. and would erase everything!
	    sed -i -e '/^ *%setup/s/$/ -T -D/' $pkgconf_dir/${spec_file}

            # repo_targz is the version-only one...
	    ( set -x; rpmbuild -ba $rpmbuild_opts --define "_topdir $(realpath .)" --define "buildsubdir ../../" $pkgconf_dir/$spec_file "$@" || true; )
            echo "RPM binary package(s) from $spec_file ------------------------------------"
            ( find RPMS/*/*.rpm | grep -v '[-]debug' | xargs mv -fv -t ${PKG_DIR}; ) ||
		die "ERROR: rpmbuild compile command failed"
	    ;;

	(*/???build)
            echo "Fake building $pkg_type package in $ctxt =============================================="
            build_srcdir="$ctxt"

            [ -d $pkgconf_dir ] || die "missing call to gen_pkg_build_config"
            # erase old one if a distinct dir...
            [ $build_srcdir/$pkgconf_dir/. -ef $build_srcdir/. ] || 
               rm -rf $build_srcdir/$pkgconf_dir
            mv $pkgconf_dir $build_srcdir/. ||
               die "directory $build_srcdir under $pkg_bldroot is missing or a broken link"
            cd $build_srcdir/. ||
               die "directory $build_srcdir under $pkg_bldroot is missing or a broken link"

            find $pkgconf_dir/*.src | while read i; do do_file_subst $i; done ||
                    die "missing call to gen_pkg_environ() or failed substitutions"
            rm -f $pkgconf_dir/*.src $pkgconf_dir/build*

            case $pkg_bldroot in
                (*/debbuild)
                    [ -n "$(command -v dpkg-buildpackage)" ] ||
                        die "ERROR: dpkg-buildpackage command was not found.  Cannot build without package \"dpkg-dev\" installed."
                    [ -s $pkgconf_dir/control -a -s $pkgconf_dir/rules ] ||
                        die "ERROR: no $pkgconf_dir/control file is ready in $pkg_bldroot/$build_srcdir/$pkgconf_dir"

                    chmod ug+x $pkgconf_dir/rules 2>/dev/null
                    dpkg-buildpackage -rfakeroot -uc -b ||
                        die "ERROR: dpkg-buildpackage compile command failed"
                    # Create unsigned packages
                    ls 2>/dev/null ../*deb | xargs -r mv -fv -t ${PKG_DIR}
                    echo "$pkgconf_dir package(s) from $build_srcdir ---------------------------------------";
                    ;;

                (*/pkgbuild)
                    [ -n "$(command -v pkgproto)" ] ||
                        die "ERROR: pkgproto command was not found.  Cannot build without package \"dpkg-dev\" installed."
                    [ -s $pkgconf_dir/makefile.build ] ||
                        die "ERROR: no $pkgconf_dir/makefile.build file is ready in $pkg_bldroot/$build_srcdir/$pkgconf_dir"

                    $MAKE -f $pkgconf_dir/makefile.build clean || die "failed during $pkgconf_dir/makefile.build clean"
                    $MAKE -f $pkgconf_dir/makefile.build build || die "failed during $pkgconf_dir/makefile.build build"
                    $MAKE -f $pkgconf_dir/makefile.build binary || die "failed during $pkgconf_dir/makefile.build build"
                    ls 2>/dev/null *.p5p *.pkg | xargs -r mv -fv -t ${PKG_DIR}
                    echo "$pkgconf_dir package(s) from $build_srcdir ---------------------------------------";
                    ;;

                (*/tarbuild)
                    [ -s $pkgconf_dir/makefile.build ] ||
                        die "ERROR: no $pkgconf_dir/makefile.build file is ready in $pkg_bldroot/$build_srcdir/$pkgconf_dir"

                    $MAKE -f $pkgconf_dir/makefile.build clean || die "failed during $pkgconf_dir/makefile.build clean"
                    $MAKE -f $pkgconf_dir/makefile.build build || die "failed during $pkgconf_dir/makefile.build build"
                    $MAKE -f $pkgconf_dir/makefile.build binary || die "failed during $pkgconf_dir/makefile.build build"
                    ls 2>/dev/null *.tar.gz | xargs -r mv -fv -t ${PKG_DIR}
                    echo "$pkgconf_dir package(s) from $build_srcdir ---------------------------------------";
                    ;;
            esac
            ;;

	 (*) die "Unknown packaging for resources for $pkg_bldroot"
	    ;;
    esac
    true
    ) || exit $?
}

do_package() {
    local ctxt=$1
    local spec_file
    local build_srcdir
    local hooks
    local repo_targz
    shift

    [ -d $pkg_bldroot ] ||
	die "missing call to gen_pkg_build_config() or missing $pkg_bldroot directory"
    (
    cd $pkg_bldroot

    case $pkg_bldroot in
	(*/rpmbuild)
            echo "Building rpm package from $ctxt =============================================="
            spec_file=$ctxt
            repo_targz=SOURCES/${PKG_NAME_VER}.tar.gz

            rm -rf BUILD/${PKG_NAME_VER}
            tar -xzvf $repo_targz -C BUILD \
               ${PKG_NAME_VER}/./{FULL_VERSION,PKG_REV,REV,LONG_BRANCH,packaging} ||
             	 die "missing call to gen_pkg_environ() or malformed $repo_targz file"

             # NOTE: must be in the BUILD/xxxx dir to subsitute anything correctly
            ( cd BUILD/${PKG_NAME_VER}; find ../../$pkgconf_dir/*.src | while read i; do do_file_subst $i; done; ) ||
                    die "missing call to gen_pkg_environ() or failed substitutions"
            rm -f $pkgconf_dir/*.src $pkgconf_dir/build*

            # move files to BUILD only when needed

            [ -s $pkgconf_dir/$spec_file ] ||
		die "ERROR: $pkgconf_dir/$spec_file is not present for packaging step"
            [ -n "$(command -v rpmbuild)" ] ||
		die "ERROR: rpmbuild command was not found.  Cannot build without package \"rpm-build\" installed."
	    [ -s $repo_targz ] ||
		die "ERROR: missing call to gen_pkg_environ()"

            # NOTE: all files above are erased as old BUILD file is erased in rpmbuild

	    ( set -x; rpmbuild -ba $rpmbuild_opts --define "_topdir $(realpath .)" $pkgconf_dir/$spec_file "$@"; )
            echo "RPM binary package(s) from $spec_file ------------------------------------"
            ( ls RPMS/*/*.rpm | grep -v '[-]debug' | xargs mv -fv -t ${PKG_DIR}; ) ||
		die "ERROR: rpmbuild compile command failed"
	    ;;

         (*/???build)
            echo "Building $pkgconf_dir package in $ctxt =============================================="
            build_srcdir="$ctxt"

            [ -d $pkgconf_dir ] || die "missing call to gen_pkg_build_config"
            mv $pkgconf_dir $build_srcdir/. ||
               die "directory $build_srcdir under $pkg_bldroot is missing or a broken link"
            cd $build_srcdir/. ||
               die "directory $build_srcdir under $pkg_bldroot is missing or a broken link"

            find $pkgconf_dir/*.src | while read i; do do_file_subst $i; done ||
                    die "missing call to gen_pkg_environ() or failed substitutions"
            rm -f $pkgconf_dir/*.src $pkgconf_dir/build*

            case $pkg_bldroot in
                (*/debbuild)
                    [ -n "$(command -v dpkg-buildpackage)" ] ||
                        die "ERROR: dpkg-buildpackage command was not found.  Cannot build without package \"dpkg-dev\" installed."
                    [ -s $pkgconf_dir/control -a -s $pkgconf_dir/rules ] ||
                        die "ERROR: no $pkgconf_dir/control file is ready in $pkg_bldroot/$build_srcdir/$pkgconf_dir"

                    chmod ug+x $pkgconf_dir/rules 2>/dev/null
                    dpkg-buildpackage -rfakeroot -uc -b ||
                        die "ERROR: dpkg-buildpackage compile command failed"
                    # Create unsigned packages
                    ls 2>/dev/null ../*deb | xargs -r mv -fv -t ${PKG_DIR}
                    echo "$pkgconf_dir package(s) from $build_srcdir ---------------------------------------";
                    ;;

                (*/pkgbuild)
                    [ -n "$(command -v pkgproto)" ] ||
                        die "ERROR: pkgproto command was not found.  Cannot build without package \"dpkg-dev\" installed."
                    [ -s $pkgconf_dir/makefile.build ] ||
                        die "ERROR: no $pkgconf_dir/makefile.build file is ready in $pkg_bldroot/$build_srcdir/$pkgconf_dir"

                    $MAKE -f $pkgconf_dir/makefile.build clean || die "failed during $pkgconf_dir/makefile.build clean"
                    $MAKE -f $pkgconf_dir/makefile.build build || die "failed during $pkgconf_dir/makefile.build build"
                    $MAKE -f $pkgconf_dir/makefile.build binary || die "failed during $pkgconf_dir/makefile.build build"
                    ls 2>/dev/null *.p5p *.pkg | xargs -r mv -fv -t ${PKG_DIR}
                    echo "$pkgconf_dir package(s) from $build_srcdir ---------------------------------------";
                    ;;

                (*/tarbuild)
                    [ -s $pkgconf_dir/makefile.build ] ||
                        die "ERROR: no $pkgconf_dir/makefile.build file is ready in $pkg_bldroot/$build_srcdir/$pkgconf_dir"

                    $MAKE -f $pkgconf_dir/makefile.build clean || die "failed during $pkgconf_dir/makefile.build clean"
                    $MAKE -f $pkgconf_dir/makefile.build build || die "failed during $pkgconf_dir/makefile.build build"
                    $MAKE -f $pkgconf_dir/makefile.build binary || die "failed during $pkgconf_dir/makefile.build build"
                    ls 2>/dev/null *.tar.gz | xargs -r mv -fv -t ${PKG_DIR}
                    echo "$pkgconf_dir package(s) from $build_srcdir ---------------------------------------";
                    ;;
            esac
            ;;

	 (*) die "Unknown packaging for resources for $pkg_bldroot"
	    ;;
    esac
    true
    ) || exit $?
}

###### WARNING: keeping for the oldest builds only
get_svn_info() {
    local branch_field

    # The field to cut changes per repository.
    if grep "svn.zmanda.com" vcs_repo.info >/dev/null; then
        branch_field=5
    elif grep "sf.net" vcs_repo.info >/dev/null; then
        branch_field=8
    fi

    SVN_PATH=`grep URL: vcs_repo.info|cut -d "/" -f $branch_field-`
    [ -z "$SVN_PATH" ] || die "subversion SVN_PATH= .from /vcs_repo.info failed"
    echo "svn_path: $SVN_PATH"

    REV=`grep Revision: vcs_repo.info|cut -d: -f 2|cut -c2-`
    [ -z "$REV" ] || die "subversion REV= failed from ./vcs_repo.info failed"
    echo "svn rev: '$REV'"

    # TYPE can be branches, trunk, or tags.
    TYPE=`echo "${SVN_PATH}"|cut -d "/" -f 1`
    [ -z "$TYPE" ] || die "subversion TYPE= failed from $SVN_PATH"

    # Set the branch name that goes in a version string
    if test "$TYPE" = "branches" -o "$TYPE" = "tags"; then
        # Strip posible "zmanda_" prefix used in SF to indicate tags
	BRANCH=`echo "${SVN_PATH}"| cut -d "/" -f 2| sed "s/zmanda_//"`
	LONG_BRANCH="$TYPE/$BRANCH"
    else
	BRANCH="trunk"
	LONG_BRANCH="$BRANCH"
    fi
}

set_zmanda_version() {
    detect_package_vars  # in case its not set
    eval "$(get_version_evalstr "$1")"
    # [ -f "$VERSION_TAR" ] || die "failed to create VERSION_TAR file"
    echo -n "$VERSION" > $src_root/FULL_VERSION
    echo -n "$LONG_BRANCH" > $src_root/LONG_BRANCH
    echo -n "$REV" > $src_root/REV
    echo -n "$PKG_REV" > $src_root/PKG_REV
}


# detect missing variables from calling script location
detect_package_vars() {
    local localpkg_suffix=
    local pkgdirs_top_top=

    declare >&/dev/null -p pkgconf_dir \
          buildpkg_dir \
          pkg_bldroot \
          pkg_name_pkgtime \
          pkgdirs_top && 
      return 0

    # check every time ...
    if ! detect_pkgdirs_top; then
        # use a default for pkg_type plus pkgdirs_top
        if [ -z "$pkg_type" -a -x $pkgdirs_top/common/substitute.pl ]; then
            localpkg_suffix=$(cd $src_root; $pkgdirs_top/common/substitute.pl <(echo %%PKG_SUFFIX%%) /dev/stdout);
            declare -g pkg_type=${localpkg_suffix##*.}
        fi
        pkg_typedir=${pkg_type/#pkg/sun-pkg}
        declare -g buildpkg_dir="${pkgdirs_top}/${pkg_typedir}"
        declare -g pkg_typedir="${pkg_typedir}"
        [ -d $pkgdirs_top/. -a -d $buildpkg_dir ] || exit 1
    fi

   detect_build_dirs

    if ! declare >&/dev/null -p pkg_name_pkgtime; then
        src_root_top=$(cd $src_root; git rev-parse --show-toplevel)
        pkgdirs_top_top=$(cd $pkgdirs_top; git rev-parse --show-toplevel)

        git_srcroot_args="--git-dir=$src_root_top/.git --work-tree=$src_root_top"
        git_pkgdirs_args="--git-dir=$pkgdirs_top_top/.git --work-tree=$pkgdirs_top_top"

        # detect time stamp from areas touched by this script

        detect_root_pkgtime
    fi

    [ -n "$pkg_name" ] && export pkg_name
    [ -n "$pkg_type" ] && export pkg_type

    export pkgconf_dir \
      buildpkg_dir \
      pkg_bldroot \
      pkg_name_pkgtime \
      pkgdirs_top
}

# detect_package_vars

set -${setopt/s}
# End Build functions
