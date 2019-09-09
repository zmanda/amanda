#!/bin/bash
# Common Functions

# Required variables:
# LOGFILE
# SYSCONFDIR
# os

type -p realpath >/dev/null || eval 'realpath() { [ $1 = -e ] && shift; ( cd $1; builtin pwd -P; ); }'

if type die 2>/dev/null >&2; then
    :
else
die() {
   echo "$@" 1>&2
   exit -1
}
fi

# find abs top-dir path
src_root="$(realpath .)"

set_script_pkg_root() {
    # compute pkg_root relative path.. (if not set)
    pkg_root_rel=$1
    [ -e "$pkg_root_rel" ] || pkg_root_rel="."

    rel_readlink="$(readlink $pkg_root_rel)"

    # change to full-path or absolute *first*
    if [ -L "$pkg_root_rel" -a $rel_readlink = ${rel_readlink#/} ]; then
       [ -f $pkg_root_rel ] && pkg_root_rel="$(dirname $pkg_root_rel)"
       pkg_root_rel=$pkg_root_rel/$rel_readlink
    elif [ -L "$pkg_root_rel" ]; then
       pkg_root_rel=$rel_readlink
    fi

    # change from file-based symlink to dir based now
    [ -f $pkg_root_rel ] && pkg_root_rel="$(dirname $pkg_root_rel)"

    # don't use full-abs ... as it may be symlinked-submodule!
    pkg_root_rel="$(realpath $pkg_root_rel)"
    pkg_root_rel="${pkg_root_rel#${src_root}/}"
    pkg_root_rel="${pkg_root_rel#${PWD}/}"

    if [ "${pkg_root_rel##*/}" = common -a -d packaging/rpm/. ]; then
        pkg_root_rel=packaging/.
        [ -s /etc/redhat-release ] && pkg_root_rel=packaging/rpm
        [ -s /etc/debian_version ] && pkg_root_rel=packaging/deb
    	pkg_type=${pkg_type:-${pkg_root_rel##*/}}
    elif [ -z "$pkg_type" -a "${pkg_root_rel##*/}" = "$pkg_root_rel" ]; then
        [ -s /etc/redhat-release ] && pkg_type=rpm
        [ -s /etc/debian_version ] && pkg_type=deb
    else
    	pkg_type=${pkg_type:-${pkg_root_rel##*/}}
    fi

    # remove all-but-last for type-of-package
    declare -g pkg_type=$pkg_type
    declare -g pkg_root=$pkg_root_rel
}

detect_pkg_name_type() {
    # remove .../packaging/ prefix
    pkg_name=${pkg_root#*/packaging/}
    if [ $pkg_name != $pkg_root ]; then
	# remove package type name
	pkg_name=${pkg_name%/rpm}
	pkg_name=${pkg_name%/deb}
	pkg_name=${pkg_name%/sun-pkg}
	pkg_name=${pkg_name%/bitrock}
	# use dir above package type
	pkg_name=${pkg_name##*/}
    else
	# packaging is *not* in this script's path
	remote_repo=$(git name-rev --name-only --refs=remotes/*/* --exclude=*/HEAD HEAD)
	[ "$remote_repo" = undefined ] &&
	    remote_repo=$(git describe --all --exclude */HEAD --match '*/*' HEAD)
	remote_repo=${remote_repo#refs/}
	remote_repo=${remote_repo#remotes/}
	remote_repo=${remote_repo%%/*}
	remote_repo=$(git remote get-url $remote_repo)
	remote_repo=${remote_repo##*/}
	remote_repo=${remote_repo%.git}
	pkg_name=$remote_repo
    fi

    declare -g pkg_name=$pkg_name
    
    # try one more time to use pkg_name to derive pkg_type
    [ "${pkg_root#*/$pkg_name/}" != $pkg_root ] && 
	declare -g pkg_type="${pkg_root#*/$pkg_name/}"
}

set_pkg_naming() {
    case "$pkg_type-$pkg_name" in 
        rpm-amanda-core|rpm-amanda?enterprise) 
                              dir_name="amanda-enterprise"
			       pkg_name="amanda_enterprise" 
			       repo_name="amanda-core"
		;;
        deb-amanda-core|deb-amanda?enterprise) 
                              dir_name="amanda-enterprise"
			       pkg_name="amanda-enterprise" 
			       repo_name="amanda-core"
		;;

        rpm-amanda-extensions|rpm-amanda?enterprise-extensions|deb-amanda-extensions|deb-amanda?enterprise-extensions) 
                              dir_name="amanda-extensions"
			       pkg_name="amanda_enterprise-extensions" 
			       repo_name="amanda-extensions"
		;;
        rpm-zmc-ae-new|rpm-amanda-zmc)
                              dir_name="amanda-zmc"
		               pkg_name="amanda-zmc" 
			       repo_name="zmc-ae-new"
		;;
        deb-zmc-ae-new|deb-amanda-zmc)
        		       pkg_name="amanda-zmc" 
			       repo_name="zmc-ae-new"
		;;

	deb-*) die "debian pkg unsupported: \"$pkg_type-$pkg_name\"";;
	rpm-*) die "rpm pkg unsupported: \"$pkg_type-$pkg_name\"";;

        sun-pkg*) true ;; 
        common-*) true
	    ;; 
    esac

    # re-use path leading to package name .. as needed
    

    declare -g pkg_name=$pkg_name
    declare -g repo_name=$repo_name
    declare -g pkg_name_dir="${pkg_root_abs%$dir_name/*}$dir_name"
}

detect_root_pkgtime() {
    a=0
    b=0

    pkg_name_pkgtime="$(cd $src_root; git log --author-date-order --pretty='%ad' --date=raw -1)"
    pkg_name_pkgtime="$(( ${pkg_name_pkgtime% *} + 0 ))"
    src_root_pkgtime=$pkg_name_pkgtime;

    [ -d $pkg_name_dir ] && pkg_dir_pkgtime=$(cd $pkg_name_dir; git log --author-date-order --pretty='%ad' --date=raw -1 .)
    [ -d $src_root/packaging/common ] && pkg_common_pkgtime=$(cd $src_root/packaging/common; git log --author-date-order --pretty='%ad' --date=raw -1 .)

    pkg_dir_pkgtime=$(( ${pkg_dir_pkgtime% *} + 0 ))
    pkg_common_pkgtime=$(( ${pkg_common_pkgtime% *} + 0 ))

    [ $pkg_dir_pkgtime -gt $pkg_name_pkgtime ] && pkg_name_pkgtime=$pkg_dir_pkgtime
    [ $pkg_common_pkgtime -gt $pkg_name_pkgtime ] && pkg_name_pkgtime=$pkg_common_pkgtime

    declare -g pkg_name_pkgtime=$pkg_name_pkgtime
}

logger() {
	# A non-annoying way to log stuff
	# ${@} is all the parameters, also known as the message.  Quoting the input
	# preserves whitespace.
	msg="`date +'%b %d %Y %T'`: ${@}"
	echo "${msg}" >> ${LOGFILE}
}

log_output_of() {
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
    # get dir-name-above-script
    # Do substitutions..
    for file; do
	[ -r "${file}" ] ||
		{ echo "substitution source file not found: $file"; return -1; }
	target="$(dirname "${file}")/$(basename ${file} .src)"
	[ "$file" != "$target" ] ||
		{ echo "substitution \"$file\" was not intended for substitution ["$target"]"; return -1; }
	pkg_type=${pkg_type} perl $src_root/$pkg_root/../common/substitute.pl \
		${file} ${target} ||
		{ echo "substitution of \"$file\" -> \"$target\" failed somehow"; return -1; }
    done
    return 0
}

get_version() {
    t=$(mktemp)
    echo "%%VERSION%%" > $t.src
    do_file_subst $t.src ||
	die "substitution of $t.src failed";
    VERSION=`cat $t`
    PKG_NAME_VER="$pkg_name-$VERSION"
    rm -f $t.src $t
}

branch_version_name() {
    v="$1"
    v=$(echo "${v##*/}"| sed -r -e 's/-[0-9]+-g[a-f0-9]+$//')
    v=$(echo "$v"| sed -r -e 's/[_.]+/^/g')
    # remove text before version numbers found
    v=$(echo "${v}"| sed -r -e 's/^[^0-9]*([0-9]+\^)/\1/')
    # apply ^ removal repeatedly until none more found
    v=$(echo "${v}"| sed -r -e ':start; s/([0-9])+\^([0-9])+/\1.\2/; tstart' | sed -e 'y,^,.,')
    echo $v
}

gen_pkg_build_config() {
    setup_dir=$1

    [ -d $setup_dir ] ||
	die "ERROR: gen_pkg_build_config() \"$setup_dir\" bad setup directory to "
    [ $pkg_root/../$pkg_type -ef $pkg_root ] ||
	die "ERROR: gen_pkg_build_config() invoked by script [$0] --> [$pkg_root] outside of $pkg_type dir"

    # Check for the packaging dirs.
    if [ -z "$PKG_DIR" ]; then
	export PKG_DIR=$src_root
    fi
    if [ ! -d ${PKG_DIR} ]; then
	mkdir -p ${PKG_DIR} ||
	   die "top directory for build PKG_DIR=${PKG_DIR} cannot be created"
    fi
    # ---------------------------------------------------
    cd ${PKG_DIR}

    mkdir -p $build_dir

    case $build_dir in
	(rpmbuild) 
	    echo "Config rpm package from $setup_dir =============================================="
	    mkdir -p $build_dir/{SOURCES,SRPMS,SPECS,BUILD,RPMS,BUILDROOT} || 
		   die "top directories for build under $(realpath .)/$build_dir cannot be created."
	    # Copy files into rpmbuild locations
	    [ -z "$(ls 2>/dev/null $setup_dir/*.spec.src)" ] || cp -vf $setup_dir/*.spec.src $build_dir/SPECS || 
		die "failed to copy spec files ($setup_dir/*.spec.src) to $build_dir/SPECS"; 
	    [ -z "$(ls 2>/dev/null $setup_dir/*.spec)" ] || cp -vf $setup_dir/*.spec $build_dir/SPECS || 
		die "failed to copy spec files ($setup_dir/*.spec) to $build_dir/SPECS"; 
	    ;;
	(debbuild) 
	    echo "Config deb package from $setup_dir =============================================="
	    rm -rf $build_dir/debian
	    cp -av $setup_dir $build_dir/debian || 
		die "failed to copy all files from $setup_dir to $build_dir/debian"; 

	    if [ -r $build_dir/debian/control-multi-arch ]; then 
		mv $build_dir/debian/control-multi-arch $build_dir/debian/control

                type dpkg-query >/dev/null 2>&1 || 
		    die "need command dpkg-query to use debian/control-multi-arch control file"; 
		dpkg_ver=$(dpkg-query --show --showformat '${Version}\n' dpkg)
		if dpkg --compare-versions "$dpkg_ver" lt 1.16; then
		    [ -r $build_dir/debian/control-single-arch ] && 
			mv -f $build_dir/debian/control-single-arch $build_dir/debian/control
		fi
	    fi

	    [ -r $build_dir/debian/control ] ||
		die "debian control file was not present in $setup_dir nor selected automatically"; 
	    ;;
	(sun-pkgbuild) 
	    echo "Config sun-pkg from $setup_dir =============================================="
            export INSTALL_DEST=$src_root/sun-pkgbuild/install
            export PKG_DEST=$src_root/sun-pkgbuild/pkg
            mkdir -p $build_dir $build_dir/install $build_dir/pkg || 
		die "failed to create $build_dir and other directories."; 
            ;;

	 (*) die 'Unknown packaging for resources';;
    esac

    # ---------------------------------------------------
    cd $src_root
}

gen_top_environ() {
    # simulate the top directory as the build one...
    cd ${PKG_DIR}

    eval "$(save_version HEAD)"

    [ -d $build_dir ] || 
	die "missing call to gen_pkg_build_config() or missing ${PKG_DIR:-\"<missing>\"}/$build_dir directory"

    case $build_dir in
	(rpmbuild) 
	    ln -sf $(realpath .) $build_dir/BUILD/$PKG_NAME_VER
            gzip -c < $VERSION_TAR > $build_dir/SOURCES/${PKG_NAME_VER}.tar.gz
	    ;;
	(debbuild) 
	    # simulate the top directory as the build one...
	    ln -sf $(realpath .) $build_dir/$PKG_NAME_VER
	    ;;
	(sun-pkgbuild) 
	    #
	    # same as gen pkg environ
            ln -sf $(realpath .) $build_dir/build
            [ -f Makefile ] && make distclean
            bash autogen
	    ;;
  	(*) die "Unknown packaging for resources in $PKG_DIR/$build_dir"
	    ;;
    esac
    cd $src_root
}

gen_pkg_environ() {
    cd ${PKG_DIR}

    eval "$(save_version HEAD)"

    tmp=$(mktemp -d)
    rm -f $tmp/${PKG_NAME_VER}
    ln -sf ${PKG_DIR} $tmp/${PKG_NAME_VER} 

    [ -d $build_dir ] ||
    	die "missing call to gen_pkg_build_config() or missing ${PKG_DIR:-\"<missing>\"}/$build_dir directory"

    case $build_dir in
	(rpmbuild) 
            rm -f $build_dir/SOURCES/${PKG_NAME_VER}.tar 
	    tar -cf $build_dir/SOURCES/${PKG_NAME_VER}.tar \
		   --exclude=*.rpm \
		   --exclude=*.deb \
		   --exclude=.git \
		   --exclude=*.tar.gz \
		   --exclude=*.tar \
		   --exclude=${build_dir} \
		    -C $tmp ${PKG_NAME_VER}/. ||
			die "tar creation from $(readlink ${PKG_NAME_VER} || echo "<missing symlink>") failed"
            #
	    # ready for the spec file to untar it
            gzip -f $build_dir/SOURCES/${PKG_NAME_VER}.tar
	    ;;
	(debbuild) 
            rm -rf $build_dir/$PKG_NAME_VER
	    tar -cf - \
		   --exclude=*.rpm \
		   --exclude=*.deb \
		   --exclude=.git \
		   --exclude=*.tar \
		   --exclude=*.tar.gz \
		   --exclude=$build_dir \
		    -C $tmp $PKG_NAME_VER/. |
		tar -xf - -C $build_dir ||
		    die "tar-based copy from $(readlink ${PKG_NAME_VER} || echo "<missing symlink>") to $PKG_DIR/$build_dir failed"
            #
	    # ready for the build system to use it
            #
	    ;;
	(sun-pkgbuild) 
	    #
	    # same as top build
            ln -sf ${PWD} $build_dir/build
            [ -f Makefile ] && make distclean
            bash autogen
            ;;

	 (*) die "Unknown packaging for resources in $PKG_DIR/$build_dir"
	    ;;
    esac

    # ---------------------------------------------------
    cd $src_root
    rm -f $tmp/${PKG_NAME_VER} 
}

gen_repo_pkg_environ() {
    repo_name=$1
    repo_ref=$2

    [ -n "$repo_name" ] || 
	die "ERROR: usage: <repo-name> <git-ref>, w/first missing";
    [ -n "$repo_ref" ] ||
	die "ERROR: usage: <repo-name> <git-ref>, w/second missing";

    remote=$(get_repo_remote $repo_name)

    [ -n "$remote" ] || die "ERROR: could not use $repo_name to create a remote repo name"

    # halt things now if there was an error in the output
    git remote get-url "$remote" || die "ERROR: could not use $repo_name to create a remote repo name"

    # in case we had a full-path name for our branch
    repo_ref=${repo_ref#remotes/}
    repo_ref=${repo_ref#$remote/}

    echo "setup attetmpt: $remote/$repo_ref"
    set -xv
    save_version $remote/$repo_ref
    set +xv

    eval "$(save_version $remote/$repo_ref)"

    cd ${PKG_DIR}/$build_dir

    set -e
    case $build_dir in
	(rpmbuild) 
            repo_tar=SOURCES/${PKG_NAME_VER}.tar
            repo_targz=SOURCES/${PKG_NAME_VER}.tar.gz
            rm -f $repo_tar $repo_targz
            git archive --remote=file://$(realpath .)/.. --format=tar.gz --prefix="$PKG_NAME_VER/./" -o $repo_targz $remote/$repo_ref ||
		die "ERROR: failed: git archive --format=tar.gz --prefix=\"$PKG_NAME_VER/./\" -o $repo_targz $remote/$repo_ref"
            gunzip $repo_targz ||
		die "ERROR: failed: decompress of $build_dir/$repo_targz"
	    # not needed because another overwrites previous one?
            tar --delete -vf $repo_tar ${PKG_NAME_VER}/./000-external || true

            # append versioning files...
            tar -Avf $repo_tar $VERSION_TAR ||
		die "ERROR: failed to append extra files to $build_dir/$repo_tar"

	    mkdir -p ${PKG_NAME_VER}
            ln -sf ../../../000-external $PKG_NAME_VER/000-external
            tar -rf $repo_tar $PKG_NAME_VER/./000-external
	    rm -rf ${PKG_NAME_VER}

            gzip $repo_tar
	    ;;
	(debbuild)
            rm -rf $PKG_NAME_VER
            git archive --remote=file://$(realpath .)/.. --format=tar --prefix="$PKG_NAME_VER/" $remote/$repo_ref |
		tar -xf - --exclude 000-external ||
		die "ERROR: failed: git archive --format=tar $remote/$repo_ref to tar -xf in $build_dir"
	    ln -sf ../../000-external "$PKG_NAME_VER/000-external"

            tar -xvf $VERSION_TAR ||
		die "ERROR: failed: tar extract of $VERSION_TAR into $build_dir/$PKG_NAME_VER"
	    ;;
	(sun-pkgbuild) 
            rm -rf build
            git archive --remote=file://$(realpath .)/.. --format=tar --prefix="build/" $remote/$repo_ref |
		tar -xf - --exclude 000-external || 
		die "ERROR: failed: git archive --format=tar.gz -o $targz $remote/$repo_ref to tar -xf"
	    ln -sf ../000-external build 

            tar -xvf $VERSION_TAR --strip-components=1 -C build ||
		die "ERROR: failed: tar extract of $VERSION_TAR into $build_dir/build"

            ( cd build; bash autogen; ) ||
		die "ERROR: failed autogen for $build_dir/build"
            ;;

	 (*) die "Unknown packaging for resources of $build_dir"
	    ;;
    esac

    cd $src_root
    rm -f $VERSION_TAR
    set +e
}

do_top_package() {
    cd ${PKG_DIR}/$build_dir || 
	die "missing call to gen_pkg_build_config() or missing ${PKG_DIR:-\"<missing>\"}/$build_dir directory"

    case $build_dir in
	(rpmbuild) 
            echo "Fake-Building via $1 =============================================="
            spec_file=$1
            shift
            targz=SOURCES/${PKG_NAME_VER}.tar.gz

            # pre-extract version info to do subst (must have /./ as if top was symlink)
            if [ -s SPECS/${spec_file}.src ]; then
               (
                 cd BUILD/${PKG_NAME_VER} ||
		 	die "directory or symlink BUILD/${PKG_NAME_VER} missing"
                 src_root=$(realpath .); 
                 pkg_root=packaging/$pkg_type
                 do_file_subst ${PKG_DIR}/$build_dir/SPECS/${spec_file}.src && rm -f ${PKG_DIR}/$build_dir/SPECS/${spec_file}.src
               )
            fi

            [ -s SPECS/$spec_file ] ||
		die "ERROR: SPECS/$spec_file is not present for packaging step"
            [ -n "$(type rpmbuild)" ] || 
		die "ERROR: rpmbuild command was not found.  Cannot build without package \"rpm-build\" installed."
	    [ -f $targz ] ||
		die "ERROR: missing call to gen_top_environ()"

	    sed -i -e '/^ *%setup/d' SPECS/${spec_file}

	    rpmbuild -ba --define "_topdir $(realpath .)" SPECS/$spec_file "$@" ||
		die "ERROR: rpmbuild compile command failed"
	    ;;

	(debbuild) 
            echo "Fake building debian package in $1 =============================================="
            deb_control=debian/control
            deb_build="$1"
            shift

            [ -d debian ] || die "missing call to gen_pkg_build_config"
            mv debian $deb_build/. || die "directory $deb_build under ${PKG_DIR}/$build_dir is missing or a broken link"
            cd $deb_build/. || die "directory $deb_build under ${PKG_DIR}/$build_dir is missing or a broken link"

            set -- $(ls debian/*.src)
            if [ $# -gt 0 ]; then
                do_file_subst "$@"
            fi

            chmod ug+x debian/rules || die "chmod of ${PKG_DIR}/$build_dir/$deb_build/debian/rules failed"

            [ -n "$(type dpkg-buildpackage)" ] ||
		die "ERROR: dpkg-buildpackage command was not found.  Cannot build without package \"dpkg-dev\" installed."
            [ -s debian/control ] ||
		die "ERROR: no debian/control file is ready in ${PKG_DIR}/$build_dir/$deb_build/debian"
            dpkg-buildpackage -rfakeroot -Tbuild ||
		die "ERROR: dpkg-buildpackage compile command failed"
	    ;;

	(sun-pkgbuild) 
	    make
	    ;;
	 (*) die "Unknown packaging for resources for $build_dir"
	    ;;
    esac
    cd ${PKG_DIR}
}

do_package() {
    ctxt=$1
    cd ${PKG_DIR}/$build_dir ||
	die "missing call to gen_pkg_build_config() or missing ${PKG_DIR:-\"<missing>\"}/$build_dir directory"

    case $build_dir in
	(rpmbuild) 
            echo "Building rpm package from $ctxt =============================================="
            spec_file=$ctxt
            targz=SOURCES/${PKG_NAME_VER}.tar.gz

            # pre-extract version info to do subst (must have /./ as if top was symlink)
            if [ -s SPECS/${spec_file}.src ]; then
               (
                 rm -rf BUILD/${PKG_NAME_VER}
                 tar -xzvf $targz -C BUILD \
                   ${PKG_NAME_VER}/./{FULL_VERSION,PKG_REV,VERSION,packaging} ||
		 	die "missing call to gen_pkg_environ() or malformed $targz file"

                 cd BUILD/${PKG_NAME_VER} ||
		 	die "missing call to gen_pkg_environ() or malformed $targz file"
                 src_root=$(realpath .); 
                 pkg_root=packaging/$pkg_type
                 do_file_subst ${PKG_DIR}/$build_dir/SPECS/${spec_file}.src && rm -f ${PKG_DIR}/$build_dir/SPECS/${spec_file}.src
               )
            fi

            [ -s SPECS/$spec_file ] || 
		die "ERROR: SPECS/$spec_file is not present for packaging step"
            [ -n "$(type rpmbuild)" ] || 
		die "ERROR: rpmbuild command was not found.  Cannot build without package \"rpm-build\" installed."
	    [ -s $targz ] ||
		die "ERROR: missing call to gen_pkg_environ()"

            shift
            rpmbuild -ba --define "_topdir $(realpath .)" SPECS/$spec_file "$@" ||
		die "ERROR: rpmbuild compile command failed"
            echo "RPM package(s) from $spec_file ------------------------------------"
            mv -fv RPMS/*/*.rpm SRPMS/*rpm ${PKG_DIR}
	    ;;

	(debbuild) 
            echo "Building debian package in $ctxt =============================================="
            deb_control=debian/control
            deb_build="$ctxt"
	    shift

            [ -d debian ] || die "missing call to gen_pkg_build_config"
            mv debian $deb_build/. || die "directory $deb_build under ${PKG_DIR}/$build_dir is missing or a broken link"
            cd $deb_build/. || die "directory $deb_build under ${PKG_DIR}/$build_dir is missing or a broken link"

            set -- $(ls debian/*.src)
            if [ $# -gt 0 ]; then
                do_file_subst "$@"
            fi

            chmod ug+x debian/rules || die "chmod of ${PKG_DIR}/$build_dir/$deb_build/debian/rules failed"

            [ -n "$(type dpkg-buildpackage)" ] || 
		die "ERROR: dpkg-buildpackage command was not found.  Cannot build without package \"dpkg-dev\" installed."
            [ -s debian/control ] ||
		die "ERROR: no debian/control file is ready in ${PKG_DIR}/$build_dir/$deb_build/debian"
            dpkg-buildpackage -rfakeroot -uc -b ||
		die "ERROR: dpkg-buildpackage compile command failed"
            # Create unsigned packages

            mv -fv ../*deb ${PKG_DIR}
            echo "Debian package(s) from $deb_build ---------------------------------------";
	    ;;

#	(sun-pkgbuild) 
	 (*) die "Unknown packaging for resources for $build_dir"
	    ;;
    esac
    cd ${PKG_DIR}
}

get_svn_info() {
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

get_git_info() {
    ref=$1
    git --no-pager log $ref --max-count=1 > vcs_repo.info

    # reduce the unix date of pkging dir to Jan 1st 
    # ... and give a base64 code to each 2 minutes (or so) [range is 0:261342]
    pkgtime_name="git."

    #default branch name

    # must be able to describe this... (using remote name!)
    rmtref="$(git describe --all --always --match '*/*' --exclude '*/HEAD' $ref 2>/dev/null)"
    rmtref="${rmtref:-$(git describe --all --always --match '*/*' $ref)}"

    # lose the exact branch name but get remote name
    rmtref=${rmtref#refs/}
    rmtref=${rmtref#remotes/}
    repo=$(git remote get-url "${rmtref%/*}");
    cache_repo=
    [ -n "$repo" ] && cache_repo=$(detect_git_cache $repo)

    if [ -n "$cache_repo" ]; then
        export GIT_DIR=$cache_repo
        oref="origin/${rmtref##*/}"
    elif [ -n "$rmtref" ]; then
        oref="$rmtref"
    else
        oref=$ref
    fi

    [ $oref = "origin/HEAD" ] && oref=$ref

    if [ -s $(git rev-parse --git-dir)/shallow ]; then
        ( set -xv; git fetch --unshallow; )  # must be done.. even if slow
    fi

    REV="$pkgtime_name.git.$(git rev-parse --short $oref)"   # get short hash-version
    REV_TAGPOS=$(git name-rev --name-only --exclude=HEAD --tags $oref)
    REV_TAGPOS="${REV_TAGPOS/undefined}"
    REF_TAGPOS=${REF_TAGPOS:-$(git name-rev --name-only --tags $oref)}
    REV_TAGPOS="${REV_TAGPOS/undefined}"
    REV_TAGPOS="${REV_TAGPOS%^[0-9]*}"

    REV_TAGROOT=$(git describe --tags $oref 2>/dev/null | sed -r -e 's,-[0-9]+-g[a-f0-9]+$,,')
    REV_TAGDIST=$(git describe --tags $oref 2>/dev/null | sed -r -e 's,.*-([0-9]+)-g[a-f0-9]+$,\1,')

    REF_IDEAL=$(git name-rev --name-only --refs='origin/next*' --refs='origin/stable*' --refs='origin/integ*' --refs='origin/dev*' $oref)
    REF_IDEAL="${REF_IDEAL/undefined}"
    REF_IDEAL="${REF_IDEAL:-$(git name-rev --name-only --exclude=HEAD $oref)}"
    REF_IDEAL="${REF_IDEAL/undefined}"
    REF_IDEAL="${REF_IDEAL:-$(git name-rev --name-only $oref)}"
    REF_IDEAL="${REF_IDEAL/undefined}"

    REV_REFPOS=$(echo $REF_IDEAL | sed -e 's/\^0$//' -e 's/\~[0-9]*$//')
    REV_REFDIST=$(echo $REF_IDEAL | sed -r -e 's/\^0$//' -e 's/^[^~]+$/0/' -e 's/.*\~([0-9]*)$/\1/')

    # classify the place found... either a concrete 
    REV_PLACE="$(git rev-parse --short --symbolic-full-name "$REV_TAGPOS" 2>/dev/null)"   # get short hash-version
    REV_PLACE=${REV_PLACE:-"$(git rev-parse --short --symbolic-full-name "$REV_REFPOS" 2>/dev/null)"}
    REV_PLACE=${REV_PLACE:-"$REV"}

    unset GIT_DIR

    # in case build is tagged precisely..
    if [ -n "$REV_TAGPOS" -a "$REV_TAGPOS" != "undefined" ]; then
        # use from numbers afterward..
        BRANCH="$REV_TAGPOS"
        REV=""  # precise name for it
        LONG_BRANCH="tags/$BRANCH"
    # in case build has *no* easy name-rev name at all
    elif [ "$REV_PLACE" = "refs/heads/master" ]; then
        BRANCH="trunk"
        LONG_BRANCH="$REV.trunk"
        # use branch plus hash
    elif [ -n "$REV_REFPOS" -a x$REV_REFDIST = x0 ] && [ 0$REV_TAGDIST -gt 0 ]; then
        BRANCH="${REV_REFPOS##*/}"
        LONG_BRANCH="branches/$BRANCH"
        REV="${REV}+$REV_TAGDIST"
    elif [ -n "$REV_TAGROOT" ] && [ 0$REV_TAGDIST -gt 0 ]; then
        # use from numbers afterward..
        BRANCH="$REV_TAGROOT"
        REV="${REV/.git./.tag.}+$REV_TAGDIST"
        LONG_BRANCH="tags/$BRANCH"
    fi

    [ -z "$BRANCH" ] && { echo "ref is $ref"; exit -1; }

    if [ $(git rev-parse $ref) != $(git rev-parse HEAD) ]; then
        :
    elif GIT_WORKING_DIR=${src_root} git diff --ignore-submodules=all --quiet && 
	  GIT_WORKING_DIR=${src_root} git diff --cached --ignore-submodules=all --quiet; then
	:
    else
        REV="${REV}.edit" # unversioned changes should be noted
    fi

    # default branch name
    BRANCH="${BRANCH:-git}"

    BRANCH="${BRANCH//-/.}"  # remove - for branch name (not allowed)
    BRANCH="${BRANCH//_/.}"  # remove _ for branch name (not allowed)
}

set_pkg_rev() {
    # Check if any known package flavors are found in the variable $BRANCH.
    # If found, remove from $VERSION and set $PKG_REV
    PKG_REV=
    rev=`echo $BRANCH|grep "$flavors"`
    if [ -n "$rev" ]; then
	PKG_REV=`echo $BRANCH|sed -e "s/.*\($flavors\)/\1/"` 
    fi

    # Also check for qa## or rc## and set PKG_REV, but don't strip.
    rev=`echo $BRANCH| grep "$qa_rc"`
    if [ -n "$rev" ]; then
	PKG_REV=`echo $BRANCH|sed -e "s/.*\($qa_rc\)/\1/"`
    fi
    # Finally set a default.
    [ -z "$PKG_REV" ] && PKG_REV=$(get_yearly_tag)

    echo "Final PKG_REV value: $PKG_REV"
    # Write the file.
    echo "SET_PKG_REV : $PKG_REV"
    printf $PKG_REV > PKG_REV
}

set_version() {
    if [ "${BRANCH}" = "trunk" ]; then
        # Debian requires a digit in version identifiers.
	if [ -n "${REV}" -a -n "${VERSION}" ]; then FULL_VERSION="${VERSION}.${REV}"; 
	elif [ -n "${REV}" ]; then VERSION="0.$REV"; fi 
    else
        # VERSION should never contain package revision info, so strip any
        # flavors
        VERSION=`echo "${BRANCH}"| sed -e "s/\(.*\)$flavors/\1/"`
        # use _ or . as divisions
	VERSION=$(branch_version_name "$VERSION")
	echo $(branch_version_name "$VERSION")
        # append .$REV if present (to show it's unofficial)
	FULL_VERSION="${VERSION}${REV:+.${REV}}";
    fi

    PKG_NAME_VER="${pkg_name}-$FULL_VERSION"
}

save_version() {
    ref=$1
    # quiet!  no output until end
    get_git_info $ref >/dev/null
    set_version >/dev/null

    tmp=$(mktemp -d)

    repo_vers_dir=/tmp/${PKG_NAME_VER}
    repo_vers_tar=/tmp/${PKG_NAME_VER}-versioning.tar
    rm -rf $repo_vers_dir

    mkdir -p $repo_vers_dir
    post=${VERSION##*[^0-9.]}
    root=${VERSION%$post}
    root=${root%.}
    echo -n $root > $repo_vers_dir/VERSION
    echo -n $FULL_VERSION > $repo_vers_dir/FULL_VERSION
    ln -sf $repo_root/packaging $repo_vers_dir
    tar -cf ${repo_vers_tar} -C /tmp ${PKG_NAME_VER}/.   # *keep* the /. 
    rm -rf /tmp/${PKG_NAME_VER}

    cat <<OUTPUT
VERSION=$FULL_VERSION
FULL_VERSION=$FULL_VERSION
PKG_NAME_VER="${pkg_name}-$FULL_VERSION"
VERSION_TAR=$repo_vers_tar
OUTPUT
}


# get script-context path as pkg_root
[ -z "$pkg_root" ] && 
    set_script_pkg_root $0

pkg_root_abs="$(realpath $pkg_root)"

# use pkg_root_abs to derive pkg_name and pkg_type
[ -z "$pkg_name" -a -n "$pkg_root_abs" ] && 
    detect_pkg_name_type

# assign standard names and pkg_types (if possible)
set_pkg_naming

build_dir=${pkg_type}build

# detect time stamp from areas touched by this script
detect_root_pkgtime

[ -n "$pkg_name" ] || die "pkg_name could not be found"
[ -n "$pkg_type" ] || die "pkg_type could not be found"


# End Common functions
