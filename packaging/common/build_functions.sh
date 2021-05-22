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
# set +xv
set +o posix

die() {
   echo "$@" 1>&2
   exit 255
}

unset xargs
unset realpath
unset tar
unset git
eval "xargs() { $(command -v xargs) -r \"\$@\"; }"
eval "realpath() { $(command -v readlink) -e \"\$@\"; }"
eval "tar() { $(command -v gtar) \"\$@\"; }"
eval "git() { $(command -v git) \"\$@\"; }"

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

# set pre-defined dirs for three (four) of various builds
detect_platform_pkg_type() {
    local presets

    [ -n "$pkg_name" ] && presets+="declare -g pkg_name=\"$pkg_name\";"
    [ -n "$repo_name" ] && presets+="declare -g repo_name=\"$repo_name\";"
    [ -n "$pkg_type" ] && presets+="declare -g pkg_type=\"$pkg_type\";"

    if [ -z "$pkg_type" -a -x $pkgdirs_top/common/substitute.pl ]; then
        declare -g pkg_suffix=$(cd $src_root; $pkgdirs_top/common/substitute.pl <(echo %%PKG_SUFFIX%%) /dev/stdout);
        declare -g pkg_type=${pkg_suffix##*.}
    fi

    case $pkg_type in
       (rpm) declare -g pkgconf_dir=SPECS   \
                       buildpkg_dir=$pkgdirs_top/rpm \
                       pkg_bldroot=$src_root/rpmbuild
            ;;
       (deb) declare -g pkgconf_dir=debian  \
                       buildpkg_dir=$pkgdirs_top/deb \
                       pkg_bldroot=$src_root/debbuild
            ;;
       (pkg) declare -g pkgconf_dir=sun-pkg \
                       buildpkg_dir=$pkgdirs_top/sun-pkg \
                       pkg_bldroot=$src_root/pkgbuild
            ;;
       (whl) declare -g pkgconf_dir=.       \
                       buildpkg_dir=$pkgdirs_top/whl \
                       pkg_bldroot=$src_root/build
            ;;
    esac

    declare -g pkg_name=${pkg_name}
    declare -g repo_name=${repo_name}
    declare -g pkg_type=${pkg_type}

    [ -d $buildpkg_dir ] || { unset buildpkg_dir; set +a; return 0; }


    [ -r $buildpkg_dir/0_vars.sh ] && { . $buildpkg_dir/0_vars.sh; }

    declare -g pkg_name=${pkg_name}
    declare -g repo_name=${repo_name}
    declare -g pkg_type=${pkg_type}

    eval "$presets"
}

# set the detected "time" stamp for this build.. from the logs
detect_root_pkgtime() {
    local a=0
    local b=0
    local t="$(date +%s)"
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
    printf -- "#--------------- %-14s: %s @%s =%d \n" top-dir $(get_yearly_tag $src_root_t) $src_root_hash $src_root_t
    printf -- "#--------------- %-14s: %s @%s =%d \n" pkg-scripts $(get_yearly_tag $buildpkg_dir_t) $buildpkg_dir_hash $buildpkg_dir_t
    printf -- "#--------------- %-14s: %s @%s =%d \n" pkg-common $(get_yearly_tag $pkg_common_t) $pkg_common_hash $pkg_common_t
    } | LANG=C sort -t: -b -k2

    t=${src_root_t}
    [ $buildpkg_dir_t -gt $t ] && t=$buildpkg_dir_t
    [ $pkg_common_t -gt $t ] && t=$pkg_common_t

    declare -g pkg_name_pkgtime=$t
}
get_remote_branch_repo() {
    local ref=$1
    local remote_repo

    ref="${ref:-HEAD}"

    # can ignore trailing info for branch or tag
    remote_repo=$(git describe --all --match '*/*' --exclude '*/HEAD' $ref )

    [[ "$remote_repo" == */* ]] || die "cannot find branch leading to ref=$ref in remote system"

    remote_repo=${remote_repo#tags/*}   # disallow a non-repo tags refs
    remote_repo=${remote_repo#refs/}
    remote_repo=${remote_repo#remotes/}

    [[ "$remote_repo" == */* ]] || die "cannot find branch leading to ref=$ref in remote system"

    remote_repo=${remote_repo%%/*}
    remote_repo=$(git ls-remote --get-url $remote_repo)
    echo $remote_repo
}

get_version_evalstr() {
    local f=$(realpath ${BASH_SOURCE[0]})
    ${f%/*}/version_setup.sh $1
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
    local setup_dir
    local setup_cwd_dir
    local pkg_typedir

    setup_dir=$1
    pkg_typedir=${pkg_type}
    [ $pkg_typedir = pkg ] && pkg_typedir=sun-pkg

    [ -d $setup_dir ] ||
	die "ERROR: gen_pkg_build_config() \"$setup_dir\" bad setup directory to "
    [ $buildpkg_dir/../$pkg_typedir -ef $buildpkg_dir ] ||
	die "ERROR: gen_pkg_build_config() invoked by script [$0] --> [$buildpkg_dir] outside of $pkg_type dir"

    setup_cwd_dir=${buildpkg_dir}

    setup_dir="$(realpath -e $setup_dir)"

    # detect if setup-dir is at or below same as buildpkg_dir or not
    [ "${setup_dir}" = "${buildpkg_dir}*" ] && setup_cwd_dir=""

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
    cd $pkg_bldroot || exit 100

    case $pkg_bldroot in
	(*/rpmbuild)
	    echo "Config rpm package from $setup_dir =============================================="
	    mkdir -p {SOURCES,SRPMS,SPECS,BUILD,RPMS,BUILDROOT} ||
		   die "top directories for build under $(realpath .) cannot be created."
	    # Copy files into rpmbuild locations
            rm -rf $pkgconf_dir
	    cp -av $setup_dir $pkgconf_dir ||
		die "failed to copy spec files ($setup_dir/*.src) to $pkgconf_dir";
            # remove spurious vars files..
            rm -f $pkgconf_dir/[0-9]*
	    ;;
	(*/debbuild|*/pkgbuild)
	    echo "Config $pkg_type package from $setup_dir ${setup_cwd:+and $setup_cwd }=============================================="
	    rm -rf $pkgconf_dir
            mkdir -p $pkgconf_dir
            # first the upper level ... then the pkg-specific files will overwrite
            if [ -d "$setup_cwd_dir" ]; then
                find $setup_cwd_dir/* -type d -prune -o -print |
                  xargs -l cp -fv -t $pkgconf_dir ||
                die "failed to copy all files from $setup_cwd_dir to $pkgconf_dir"
            fi
	    cp -fav $setup_dir/* $pkgconf_dir/ ||
		die "failed to copy all files from $setup_dir to $pkgconf_dir";

	    [ $pkg_type = deb -a ! -r $pkgconf_dir/control.src -a ! -r $pkgconf_dir/control ] &&
		die "$pkgconf_dir control (nor control.src) file was not present in $setup_dir nor selected automatically";
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

    [ -d $pkg_bldroot ] ||
	die "missing call to gen_pkg_build_config() or missing $pkg_bldroot directory"

    (
    cd $pkg_bldroot || exit 100

    case $pkg_bldroot in
	(*/rpmbuild)
	    ln -sf $(realpath ..) BUILD/$PKG_NAME_VER
            # gzip -c < $VERSION_TAR > SOURCES/${PKG_NAME_VER}.tar.gz
	    ;;
	(*/debbuild|*/pkgbuild)
	    # simulate the top directory as the build one...
	    ln -sf $(realpath ..) $PKG_NAME_VER
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
	    tar -cf SOURCES/${PKG_NAME_VER}.tar \
		   --exclude=\*.rpm \
		   --exclude=\*.deb \
		   --exclude=.git \
		   --exclude=\*.tar.gz \
		   --exclude=\*.tar \
		   --exclude=debbuild \
		   --exclude=rpmbuild \
		   --exclude=pkgbuild \
		    -C $tmp ${PKG_NAME_VER}/. ||
			die "tar creation from $(readlink ${PKG_NAME_VER} || echo "<missing symlink>") failed"
            #
	    # ready for the spec file to untar it
            gzip -f SOURCES/${PKG_NAME_VER}.tar
	    ;;
	(*/debbuild|*/pkgbuild)
            rm -rf $PKG_NAME_VER
	    tar -cf - \
		   --exclude=*.rpm \
		   --exclude=*.deb \
		   --exclude=*.pkg \
		   --exclude=.git \
		   --exclude=*.tar \
		   --exclude=*.tar.gz \
		   --exclude=debbuild \
		   --exclude=rpmbuild \
		   --exclude=pkgbuild \
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

gen_repo_pkg_environ() {
    local repo_name
    local repo_ref
    local vars
    local repo_tar

    repo_name=$1
    repo_ref=$2

    [ -n "$repo_name" ] ||
	die "ERROR: usage: <repo-name> <git-ref>, w/first missing";
    [ -n "$repo_ref" ] ||
	die "ERROR: usage: <repo-name> <git-ref>, w/second missing";

    if git 2>/dev/null >&2 rev-parse --verify "tags/$repo_ref^{commit}"; then
        remote=tags
    elif [[ "$repo_ref" == __temp_merge-* ]] &&
            git 2>/dev/null >&2 rev-parse --verify "$repo_ref^{commit}"; then
        # define the remote with "self" if needed
        git 2>/dev/null >&2 remote remove __self__ || true
        git remote add __self__ $(realpath $(git rev-parse --show-toplevel)) || true
        git fetch __self__
        remote=__self__
    else
        remote=$repo_name
        [ -n "$remote" ] ||
            die "ERROR: could not use $repo_name to create a remote repo name"
        git ls-remote --get-url "$remote" >&/dev/null ||
            die "ERROR: could not use $repo_name to create a remote repo name"
        repo_ref=${repo_ref#remotes/}
        repo_ref=${repo_ref#$remote/}
    fi

    echo "setup attempt: $remote/$repo_ref"

    set_zmanda_version $remote/$repo_ref

    (
    cd $pkg_bldroot

    set -e
    case $pkg_bldroot in
	(*/rpmbuild)
            repo_tar=SOURCES/${PKG_NAME_VER}.tar
            rm -f $repo_tar

            git archive --remote=file://$(realpath ..) --format=tar --prefix="$PKG_NAME_VER/./" -o $repo_tar $remote/$repo_ref ||
		die "ERROR: failed: git archive --format=tar --prefix=\"$PKG_NAME_VER/./\" -o $repo_tar $remote/$repo_ref"

            # append versioning files...
            #tar -Avf $repo_tar $VERSION_TAR ||
	    #	die "ERROR: failed to append extra files to $pkg_bldroot/$repo_tar"

            gzip -f $repo_tar
	    ;;
	(*/debbuild|*/pkgbuild)
            rm -rf $PKG_NAME_VER
            git archive --remote=file://$(realpath ..) --format=tar --prefix="$PKG_NAME_VER/" $remote/$repo_ref |
		tar -xf - --exclude 000-external ||
		die "ERROR: failed: git archive --format=tar $remote/$repo_ref to tar -xf in $pkg_bldroot"

            #tar -xvf $VERSION_TAR ||
	    #	die "ERROR: failed: tar extract of $VERSION_TAR into $pkg_bldroot/$PKG_NAME_VER"
	    ;;
	 (*) die "Unknown packaging for resources of $pkg_bldroot"
	    ;;
    esac
    # rm -f $VERSION_TAR
    ) || exit $?

    set +e
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
	    ( set -x; rpmbuild -ba $rpmbuild_opts --define "_topdir $(realpath .)" $pkgconf_dir/$spec_file "$@" || true; )
            echo "RPM binary package(s) from $spec_file ------------------------------------"
            ( find RPMS/*/*.rpm | grep -v '[-]debug' | xargs mv -fv -t ${PKG_DIR}; ) ||
		die "ERROR: rpmbuild compile command failed"
	    ;;

	(*/debbuild)
            echo "Fake building $pkg_type package in $ctxt =============================================="
            build_srcdir="$ctxt"

            [ -d $pkgconf_dir ] || die "missing call to gen_pkg_build_config"
            mv $pkgconf_dir $build_srcdir/. ||
               die "directory $build_srcdir under $pkg_bldroot is missing or a broken link"
            cd $build_srcdir/. ||
               die "directory $build_srcdir under $pkg_bldroot is missing or a broken link"

            find $pkgconf_dir/*.src | while read i; do do_file_subst $i; done ||
                    die "missing call to gen_pkg_environ() or failed substitutions"
            rm -f $pkgconf_dir/*.src $pkgconf_dir/build*

            chmod ug+x $pkgconf_dir/rules ||
               die "could not chmod $pkgconf_dir/rules"

            [ -n "$(command -v dpkg-buildpackage)" ] ||
                die "ERROR: dpkg-buildpackage command was not found.  Cannot build without package \"dpkg-dev\" installed."
            [ -s $pkgconf_dir/control ] ||
                die "ERROR: no $pkgconf_dir/control file is ready in $pkg_bldroot/$build_srcdir/$pkgconf_dir"

            dpkg-buildpackage -rfakeroot -uc -b ||
                die "ERROR: dpkg-buildpackage compile command failed"
            # Create unsigned packages
            mv -fv ../*deb ${PKG_DIR}
            echo "$pkgconf_dir package(s) from $build_srcdir ---------------------------------------";
            ;;

	(*/pkgbuild)
            echo "Fake building $pkg_type package in $ctxt =============================================="
            build_srcdir="$ctxt"

            [ -d $pkgconf_dir ] || die "missing call to gen_pkg_build_config"
            mv $pkgconf_dir $build_srcdir/. || die "directory $build_srcdir under $pkg_bldroot is missing or a broken link"
            cd $build_srcdir/. || die "directory $build_srcdir under $pkg_bldroot is missing or a broken link"

            find $pkgconf_dir/*.src | while read i; do do_file_subst $i; done ||
                    die "missing call to gen_pkg_environ() or failed substitutions"
            rm -f $pkgconf_dir/*.src $pkgconf_dir/build*

            chmod $pkgconf_dir/makefile.build | die "could not chmod $pkgconf_dir/rules"
            [ -n "$(command -v pkgproto)" -a -n "$(command -v pkgmk)" -a -n "$(command -v pkgtrans)" ] ||
                die "ERROR: dpkg-buildpackage command was not found.  Cannot build without package \"dpkg-dev\" installed."
            [ -s $pkgconf_dir/makefile.build ] ||
                die "ERROR: no $pkgconf_dir/makefile.build file is ready in $pkg_bldroot/$build_srcdir/$pkgconf_dir"

            [ -z "$MAKE" -a -n "$(command -v gmake)" ] && MAKE=gmake
            [ -z "$MAKE" -a -n "$(command -v make)" ] && MAKE=make
            declare -g MAKE=$MAKE

            $MAKE -f $pkgconf_dir/makefile.build clean || die "failed during $pkgconf_dir/makefile.build clean"
            $MAKE -f $pkgconf_dir/makefile.build build || die "failed during $pkgconf_dir/makefile.build build"
            $MAKE -f $pkgconf_dir/makefile.build binary || die "failed during $pkgconf_dir/makefile.build build"
            mv -fv *.pkg ${PKG_DIR}
            echo "$pkgconf_dir package(s) from $build_srcdir ---------------------------------------";
            ;;

	 (*) die "Unknown packaging for resources for $pkg_bldroot"
	    ;;
    esac
    true
    )  || exit $?
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

	(*/debbuild)
            echo "Building $pkgconf_dir package in $ctxt =============================================="
            build_srcdir="$ctxt"

            [ -d $pkgconf_dir ] || die "missing call to gen_pkg_build_config"
            mv $pkgconf_dir $build_srcdir/. || die "directory $build_srcdir under $pkg_bldroot is missing or a broken link"
            cd $build_srcdir/. || die "directory $build_srcdir under $pkg_bldroot is missing or a broken link"

            find $pkgconf_dir/*.src | while read i; do do_file_subst $i; done ||
                    die "missing call to gen_pkg_environ() or failed substitutions"
            rm -f $pkgconf_dir/*.src $pkgconf_dir/build*

            chmod ug+x $pkgconf_dir/rules ||
               die "could not chmod $pkgconf_dir/rules"

            [ -n "$(command -v dpkg-buildpackage)" ] ||
                die "ERROR: dpkg-buildpackage command was not found.  Cannot build without package \"dpkg-dev\" installed."
            [ -s $pkgconf_dir/control ] ||
                die "ERROR: no $pkgconf_dir/control file is ready in $pkg_bldroot/$build_srcdir/$pkgconf_dir"

            dpkg-buildpackage -rfakeroot -uc -b ||
                die "ERROR: dpkg-buildpackage compile command failed"
            # Create unsigned packages
            mv -fv ../*deb ${PKG_DIR}
            echo "$pkgconf_dir package(s) from $build_srcdir ---------------------------------------";
	    ;;

	(*/pkgbuild)
            echo "Building $pkgconf_dir package in $ctxt =============================================="
            build_srcdir="$ctxt"

            [ -d $pkgconf_dir ] || die "missing call to gen_pkg_build_config"
            mv $pkgconf_dir $build_srcdir/. || die "directory $build_srcdir under $pkg_bldroot is missing or a broken link"
            cd $build_srcdir/. || die "directory $build_srcdir under $pkg_bldroot is missing or a broken link"

            find $pkgconf_dir/*.src | while read i; do do_file_subst $i; done ||
                    die "missing call to gen_pkg_environ() or failed substitutions"
            rm -f $pkgconf_dir/*.src $pkgconf_dir/build*

            chmod $pkgconf_dir/makefile.build | die "could not chmod $pkgconf_dir/rules"
            [ -n "$(command -v pkgproto)" -a -n "$(command -v pkgmk)" -a -n "$(command -v pkgtrans)" ] ||
                die "ERROR: dpkg-buildpackage command was not found.  Cannot build without package \"dpkg-dev\" installed."
            [ -s $pkgconf_dir/makefile.build ] ||
                die "ERROR: no $pkgconf_dir/makefile.build file is ready in $pkg_bldroot/$build_srcdir/$pkgconf_dir"

            [ -z "$MAKE" -a -n "$(command -v gmake)" ] && MAKE=gmake
            [ -z "$MAKE" -a -n "$(command -v make)" ] && MAKE=make
            declare -g MAKE=$MAKE

            $MAKE -f $pkgconf_dir/makefile.build clean || die "failed during $pkgconf_dir/makefile.build clean"
            $MAKE -f $pkgconf_dir/makefile.build build || die "failed during $pkgconf_dir/makefile.build build"
            $MAKE -f $pkgconf_dir/makefile.build binary || die "failed during $pkgconf_dir/makefile.build build"
            mv -fv *.pkg ${PKG_DIR}
            echo "$pkgconf_dir package(s) from $build_srcdir ---------------------------------------";
	    ;;

	 (*) die "Unknown packaging for resources for $pkg_bldroot"
	    ;;
    esac
    true
    ) || exit $?
}

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
        # Strip posible "zmanda_" prefix used in SF to indicate enterprise
        # tags
	BRANCH=`echo "${SVN_PATH}"| cut -d "/" -f 2| sed "s/zmanda_//"`
	LONG_BRANCH="$TYPE/$BRANCH"
    else
	BRANCH="trunk"
	LONG_BRANCH="$BRANCH"
    fi
}

set_zmanda_version() {
    eval "$(get_version_evalstr "$1")"
    # [ -f "$VERSION_TAR" ] || die "failed to create VERSION_TAR file"
    echo -n "$VERSION" > $src_root/FULL_VERSION
    echo -n "$LONG_BRANCH" > $src_root/LONG_BRANCH
    echo -n "$REV" > $src_root/REV
    echo -n "$PKG_REV" > $src_root/PKG_REV
}


# need to find the root for packaging/*
[ -z "$pkgdirs_top" -a -d "$src_root/packaging/." ] && 
    declare -g pkgdirs_top=$src_root/packaging

# detect missing variables from calling script location
detect_package_vars() {

    # check every time ...
    detect_pkgdirs_top

    declare >&/dev/null -p \
         pkg_suffix \
         pkg_name \
         pkg_type \
         pkgconf_dir \
         buildpkg_dir \
         pkg_bldroot \
         repo_name ||
   detect_platform_pkg_type

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

    export pkg_suffix \
      pkgconf_dir \
      buildpkg_dir \
      pkg_bldroot \
      pkg_name_pkgtime \
      pkgdirs_top
}

detect_package_vars
set -${setopt/s}
# End Common functions
