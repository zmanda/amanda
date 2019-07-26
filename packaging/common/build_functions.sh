#!/bin/bash
# Common Functions

# Required variables:
# LOGFILE
# SYSCONFDIR
# os

# find abs top-dir path
src_root="$(pwd -P)"

pkg_root=${0#$src_root/}
pkg_root=$(dirname $pkg_root)
pkg_root_abs="$(cd $pkg_root; pwd -P)"

# remove all-but-last for type-of-package
pkg_type=${pkg_type:-${pkg_root_rel##*/}}
build_dir=${pkg_type}build

if type die 2>/dev/null >&2; then
    :
else
die() {
   echo "$@" 1>&2
   exit -1
}
fi

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
    echo "%%VERSION%%" > /tmp/version.src
    do_file_subst /tmp/version.src ||
	die "substitution of /tmp/version.src failed";
    VERSION=`cat /tmp/version`
    PKG_NAME_VER="$pkg_name-$VERSION"
    rm -f /tmp/version.src /tmp/version
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
	PKG_DIR=$src_root
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
		   die "top directories for build under ${PWD}/$build_dir cannot be created."
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
	    ln -sf ${PWD} $build_dir/BUILD/$PKG_NAME_VER
            gzip -c < $VERSION_TAR > $build_dir/SOURCES/${PKG_NAME_VER}.tar.gz
	    ;;
	(debbuild) 
	    # simulate the top directory as the build one...
	    ln -sf ${PWD} $build_dir/$PKG_NAME_VER
	    ;;
	(sun-pkgbuild) 
	    #
	    # same as gen pkg environ
            ln -sf ${PWD} $build_dir/build
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

    rm -f /tmp/${PKG_NAME_VER}
    ln -sf ${PKG_DIR} /tmp/${PKG_NAME_VER} 

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
		   --exclude=000-external \
		   --exclude=${build_dir} \
		    -C /tmp ${PKG_NAME_VER}/. ||
			die "tar creation from $(readlink ${PKG_NAME_VER} || echo "<missing symlink>") failed"
            mkdir $PKG_NAME_VER/
            ln -sf ${PKG_DIR}/000-external $PKG_NAME_VER/000-external
            tar -rf $build_dir/SOURCES/${PKG_NAME_VER}.tar $PKG_NAME_VER/./000-external
            rm -rf $PKG_NAME_VER
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
		   --exclude=000-external \
		   --exclude=$build_dir \
		    -C /tmp $PKG_NAME_VER/. |
		tar -xf - -C $build_dir ||
		    die "tar-based copy from $(readlink ${PKG_NAME_VER} || echo "<missing symlink>") to $PKG_DIR/$build_dir failed"
            #
	    # ready for the build system to use it
	    ln -sf ${PKG_DIR}/000-external $build_dir/$PKG_NAME_VER/
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
    rm -f /tmp/${PKG_NAME_VER} 
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
                 src_root=$(pwd -P); 
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

	    rpmbuild -ba --define "_topdir ${PWD}" SPECS/$spec_file "$@" ||
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
    cd ${PKG_DIR}/$build_dir ||
	die "missing call to gen_pkg_build_config() or missing ${PKG_DIR:-\"<missing>\"}/$build_dir directory"

    case $build_dir in
	(rpmbuild) 
            echo "Building rpm package from $1 =============================================="
            spec_file=$1
            targz=SOURCES/${PKG_NAME_VER}.tar.gz

            # pre-extract version info to do subst (must have /./ as if top was symlink)
            if [ -s SPECS/${spec_file}.src ]; then
               ( 
                 rm -rf BUILD/${PKG_NAME_VER}
                 tar -xzvf $targz -C BUILD \
                   ${PKG_NAME_VER}/./FULL_VERSION ||
		 	die "missing call to gen_pkg_environ() or malformed $targz file"

                 cd BUILD/${PKG_NAME_VER} ||
		 	die "missing call to gen_pkg_environ() or malformed $targz file"
                 src_root=$(pwd -P); 
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
            rpmbuild -ba --define "_topdir ${PWD}" SPECS/$spec_file "$@" ||
		die "ERROR: rpmbuild compile command failed"
            echo "RPM package(s) from $spec_file ------------------------------------"
            mv -vn RPMS/*/*.rpm SRPMS/*rpm ${PKG_DIR}
	    ;;

	(debbuild) 
            echo "Building debian package in $1 =============================================="
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
            dpkg-buildpackage -rfakeroot -uc -b ||
		die "ERROR: dpkg-buildpackage compile command failed"
            # Create unsigned packages

            mv -vn ../*deb ${PKG_DIR}
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
    pkgtime_name=$(get_yearly_tag)

    #default branch name

    REV="$pkgtime_name.git.$(git rev-parse --short $ref)"   # get short hash-version
    REV_TAGPOS=$(git name-rev --name-only --tags $ref)
    REV_TAGROOT=$(git describe --tags $ref | sed -r -e 's,-[0-9]+-g[a-f0-9]+$,,')
    REV_REFPOS=$(git name-rev --name-only $ref | sed -e 's/\^0$//' -e 's/\~[0-9]*$//')

    # classify the place found... either a concrete 
    REV_PLACE="$(git rev-parse --short --symbolic-full-name "$REV_TAGPOS" 2>/dev/null)"   # get short hash-version
    REV_PLACE=${REV_PLACE:-"$(git rev-parse --short --symbolic-full-name "$REV_REFPOS" 2>/dev/null)"}
    REV_PLACE=${REV_PLACE:-"$REV"}

    # in case build is tagged precisely..
    if [ "$REV_TAGPOS" != "undefined" ]; then
        # use from numbers afterward..
        BRANCH="$REV_TAGPOS"
        REV=""  # precise name for it
        LONG_BRANCH="tags/$BRANCH"
    # in case build has *no* easy name-rev name at all
    elif [ $REV_PLACE = "refs/heads/master" ]; then
        BRANCH="trunk"
        LONG_BRANCH="$REV.trunk"
        # use branch plus hash
    elif [ -n "$REV_TAGROOT" ]; then
        # use from numbers afterward..
        BRANCH="$REV_TAGROOT"
        LONG_BRANCH="tags/$BRANCH"
    else
        BRANCH="${REV_REFPOS##*/}"
        LONG_BRANCH="branches/$BRANCH"
    fi

    # default branch name
    BRANCH="${BRANCH:-git}"
}

set_version() {
    if [ "${BRANCH}" = "trunk" ]; then
        # Debian requires a digit in version identifiers.
	if [ -n "${REV}" -a -n "${VERSION}" ]; then VERSION="${VERSION}.${REV}"; 
	elif [ -n "${REV}" ]; then VERSION="0.$REV"; fi 
    else
        # VERSION should never contain package revision info, so strip any
        # flavors
        VERSION=`echo "${BRANCH}"| sed -e "s/\(.*\)$flavors/\1/"`
        # use _ or . as divisions
	VERSION=$(branch_version_name "$VERSION")
	echo $(branch_version_name "$VERSION")
        # append .$REV if present (to show it's unofficial)
	VERSION="${VERSION}${REV:+.${REV}}";
    fi

    PKG_NAME_VER="${pkg_name}-$VERSION"
    echo "wrote version: $PKG_NAME_VER"
}

save_version() {
    get_git_info $1
    set_version > /dev/null

    repo_vers_dir=/tmp/${PKG_NAME_VER}
    repo_vers_tar=/tmp/${PKG_NAME_VER}-versioning.tar
    rm -rf $repo_vers_dir

    mkdir -p $repo_vers_dir
    echo -n $VERSION > $repo_vers_dir/FULL_VERSION
    tar -cf ${repo_vers_tar} -C /tmp ${PKG_NAME_VER}/.   # *keep* the /. 
    rm -rf /tmp/${PKG_NAME_VER}

    cat <<OUTPUT
VERSION=$VERSION
PKG_NAME_VER="${pkg_name}-$VERSION"
VERSION_TAR=$repo_vers_tar
OUTPUT
}
