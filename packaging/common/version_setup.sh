#!/usr/bin/env bash
#
# This script is run by extension's autogen and amanda enterprise's builddist
# to create the FULL_VERSION and PKG_REV files.  Amanda Enterprise ignores the
# contents of VERSION because it comes from community and is static.  Instead
# Amanda Enterprise uses git branch or tag names.  Amanda Enterprise
# Extensions does not have a VERSION file.
#
# If run from a tag, the value of VERSION is the branch or tag name
# reformatted.
#
# Extensions ./configure uses FULL_VERSION in the macro ZAMANDA_VERSION (from
# config/zmanda_version.m4) to avoid hard coding the a version string.
# Amanda Enterprise ./configure has an equivalent macro AMANDA_INIT_VERSION
#set -x

if [ "$(type -t get_yearly_tag)" != "function" ]; then
    . packaging/common/build_functions.sh
fi

[ -x "$(command -v gsed)" ] && eval "sed() { command gsed \"\$@\"; }"
[ -x "$(command -v gmake)" ] && eval "make() { command gmake \"\$@\"; }"
[ -x "$(command -v ggrep)" ] && eval "grep() { command ggrep \"\$@\"; }"

sed --version | grep -i 'GNU sed' >/dev/null || exit 1;

detect_git_cache() {
    local url
    local name
    local repo
    local nrepo
    local gitdir=$(git rev-parse --git-dir):

    url=$1
    name=$url
    name=${name%.git};
    name=${name##*/};

    [ "$url" -ef "$(git rev-parse --show-toplevel)" ] && {
        # no cache exists.. use local
        return
    }

    [ -n "$name" ] || return 0

    repo=/opt/src/$name/.git
    [ -d "$repo" ] || return 0
    [ ! -s "$repo/shallow" ] || return 0

    nrepo="$(GIT_DIR=$repo git ls-remote --get-url origin)"
    nrepo="${nrepo##*/}"
    nrepo="${nrepo%.git}"

    [ "$nrepo" = "$name" ] || return 0
    [ "$repo/FETCH_HEAD" -ot "$gitdir/FETCH_HEAD" ] &&
       { GIT_DIR=$repo git fetch --prune origin || return 0; }
    # if nothing happened...
    [ "$repo/FETCH_HEAD" -ot "$gitdir/FETCH_HEAD" ] &&
        touch "$repo/FETCH_HEAD"
    echo $repo
}

get_latest_git_tag() {
    local ref="$1"
    local sha="$(git 2>/dev/null rev-parse --verify $ref)"
    local tags="$(git show-ref --tags | sed -e "\|^$sha |!d" -e 's,.* refs/tags/,,')"
    [ -n "$tags" ] || return;
    {
       # prioritize fully-numeric versions of latest numeric order by decimal
       grep    '[0-9.][^0-9.]' <<<"$tags" | sort -nt.
       grep -v '[0-9.][^0-9.]' <<<"$tags" | sort -nt.
    } | tail -1
}

get_git_info() {
    local ref
    local rmtref
    local repo
    local oref
    local cache_repo
    local pkgtime_name

    ref=$1
    git --no-pager log $ref --max-count=1 > vcs_repo.info

    # reduce the unix date of pkging dir to Jan 1st
    # ... and give an alpha code [<VOWELS>consonants>] to encode the minute
    pkgtime_name=$(get_yearly_tag $pkg_name_pkgtime)

    #default branch name

    # must be able to describe this... (using remote name!)
    rmtref="$(git describe --all --match '*/*' --always --exclude '*/HEAD' $ref 2>/dev/null)"
    rmtref="${rmtref:-$(git describe --all --match '*/*' --always $ref)}"

    # can't use HEAD so try to salvage it...
    [[ $rmtref == */HEAD ]] && rmtref="${rmtref%/*}/${ref##*/}"

    if [[ $rmtref == */* ]]; then
	# lose the exact branch name but get remote name
	rmtref=${rmtref#refs/}
	rmtref=${rmtref#remotes/}
	repo=$(git ls-remote --get-url "${rmtref%/*}");
    else
	repo=$(git ls-remote --get-url origin);
        rmtref=  # signal that nothing is a remote ref
    fi

    oref=
    cache_repo=
    [ -n "$repo" ] && cache_repo=$(detect_git_cache $repo) || true

    ############################################################
    #### MAY USE GIT_DIR ENVIRONMENT OVERRIDE #####
    if [ -d "$cache_repo" -a -n "$rmtref" ]; then
       oref="origin/${rmtref##*/}" || true
       rmtref="$oref"
       export GIT_DIR=$cache_repo
    fi

    if [ -z "$oref" -a -n "$rmtref" ]; then
        oref="$rmtref"
    elif [ -z "$oref" ]; then
        oref=$ref
    fi

    [ $oref = "origin/HEAD" ] && oref=$ref

    if [ -s $(git rev-parse --git-dir)/shallow ]; then
        ( set -xv; git fetch --unshallow; )  # must be done.. even if slow
    fi

    # keep the package-time in front to alphabetize-over-time
    local year_code=$(printf %02d $(( ${pkgtime_name:0:2} + 0 )) )
    year_code=${year_code:-$(date +%y)}
    declare -g SHA=$(git rev-parse --short $oref)
    local year_break

    local REF_IDEAL

    local REV_TAGPOS
    local REV_TAGDIST
    local REV_TAGROOT

    local REV_PLACE
    local REV_REFBR
    local REV_REFDIST

    # get name-rev version of tag-based names.. but snip any '^0' at end...
    ####### find tag-based [forward-relative] info for version
    local tmpbranch="$(git name-rev --name-only --refs='__temp_merge-*' $oref)"
    local tmpsubject="$(git log -1 --pretty=%s $oref)"
    local newsha="$(git rev-parse --verify --short ${oref}^2)"
    if [ -n "$newsha" ] && 
        [[ "$tmpbranch" == __temp_merge-* ]] && 
        [[ "$tmpsubject" == Merge\ remote-tracking\ branch*into\ $tmpbranch ]]; 
    then
        SHA="$newsha"
        t=$(git log --pretty='%at' -1 $SHA)
        t=$(( t + 0 ))
        [ $t -gt $pkg_name_pkgtime ] && pkg_name_pkgtime=$t
        pkgtime_name=$(get_yearly_tag $pkg_name_pkgtime)
        # get describe-based forward-aimed tag-name (root) and distance (dist) if that works
        REV_TAGPOS=${REV_TAGPOS:-$(get_latest_git_tag ${oref}^)}
        REV_TAGROOT=$(git describe --tags ${oref}^ 2>/dev/null)
    else
        REV_TAGPOS=${REV_TAGPOS:-$(get_latest_git_tag $oref)}
        REV_TAGROOT=$(git describe --tags $oref 2>/dev/null)
        # get describe-based forward-aimed tag-name (root) and distance (dist) if that works
    fi

    REV_TAGPOS="${REV_TAGPOS%^[0-9]*}"
    REV_TAGDIST="$(sed <<<"$REV_TAGROOT" -r -e 's/^.*-([0-9]+)-g[a-f0-9]+$/\1/' -e '/[^0-9]/d')"
    # trim off tag-dist
    [ -n "$REV_TAGDIST" ] && REV_TAGROOT="${REV_TAGROOT%-$REV_TAGDIST-g[a-f0-9]*}"

    # get name-rev branch-based name and distance (and prefer certain branches if possible)
    REF_IDEAL=$(git name-rev --name-only --refs='origin/next*[0-9].[0-9]*' $oref)
    REF_IDEAL="${REF_IDEAL#undefined}"
    REF_IDEAL="${REF_IDEAL:-$(git name-rev --name-only --refs='origin/stable*[0-9].[0-9]*' $oref)}"
    REF_IDEAL="${REF_IDEAL#undefined}"
    REF_IDEAL="${REF_IDEAL:-$(git name-rev --name-only --refs='origin/integ*[0-9].[0-9]*' $oref)}"
    REF_IDEAL="${REF_IDEAL#undefined}"
    REF_IDEAL="${REF_IDEAL:-$(git name-rev --name-only --refs='origin/dev*[0-9].[0-9]*' $oref)}"
    REF_IDEAL="${REF_IDEAL#undefined}"
    REF_IDEAL="${REF_IDEAL:-$(git name-rev --name-only --exclude=HEAD --refs='origin/*[0-9].[0-9]*' $oref 2>/dev/null)}"
    REF_IDEAL="${REF_IDEAL:-$(git name-rev --name-only --refs='origin/*[0-9].[0-9]*' $oref)}"
    REF_IDEAL="${REF_IDEAL#undefined}"

    REF_IDEAL="${REF_IDEAL%^0}"

    # get branch name separated out and get distance-back from branch (or zero)
    REV_REFBR=$(sed <<<"$REF_IDEAL" -r -e 's/\~[0-9~^]*$//')
    REV_REFDIST=$(sed <<<"$REF_IDEAL" -r -e 's/^[^~]*\~([0-9]+).*$/\1/' -e '/[^0-9]/s,.*,0,g' )
    REV_REFDIST=$(printf %02d $(( REV_REFDIST + 0 )))

    if [ -n "$REV_REFBR" -a $(( ${pkgtime_name:0:2} + 2000 )) -gt 2000 ]; then
        year_break=$(printf %04d-01-01 $(( ${pkgtime_name:0:2} + 2000 )) )
        read REV_YRDIST < <(git log --pretty='%H' --until $year_break $REV_REFBR | 
                             git name-rev --name-only --refs=$REV_REFBR --stdin | 
                             sed -r -e '/^[^~]*\~[0-9]+$/!d' -e q)
        REV_YRDIST=${REV_YRDIST##*~}
        REV_YRDIST=${REV_YRDIST%%^*}  # clean up if needed
        REV_YRDIST=${REV_YRDIST//[^0-9]}
        REV_YRDIST=$(printf %04d $(( REV_YRDIST - REV_REFDIST )))
        REV_YRDIST=.y${year_code}+${REV_YRDIST%%^*}  # clean up if needed
    fi
    
    ####### officially prove a tag or branch are reachable as a base

    # test if we have a specific (master) branch
    REV_PLACE="$(git rev-parse --short --symbolic-full-name "$REV_TAGPOS" 2>/dev/null)"   # get short hash-version
    REV_PLACE=${REV_PLACE:-"$(git rev-parse --short --symbolic-full-name "$REV_REFBR" 2>/dev/null)"}

    unset GIT_DIR
    #### END OF GIT_DIR ENVIRONMENT OVERRIDE (IF PRESENT) #####
    ############################################################

    # build is tagged precisely..
    if [ -n "$REV_TAGPOS" -a -z "$REV_TAGDIST" ]; then
        BRANCH="$REV_TAGPOS"
        LONG_BRANCH="tags/$BRANCH"
        REV=""       # no extra to branch info
        PKG_REV="1"  # precise name for it.. use no hash

    # build is simply from master
    elif [ "$REV_PLACE" = "refs/heads/master" ]; then
        BRANCH="trunk"
        LONG_BRANCH="${SHA}.trunk"
        REV="$REV_YRDIST"        # add commit# for year
        PKG_REV="${pkgtime_name:3}.git.$SHA"

    # build is the tip of a branch.. so name it with tag-distance if needed
    elif [ -n "$REV_REFBR" -a $((REV_REFDIST)) = 0 ]; then
        BRANCH="${REV_REFBR##*/}"
        LONG_BRANCH="branches/$BRANCH"
        REV="$REV_YRDIST"         # add commit# for year
        PKG_REV="${pkgtime_name:3}.git.$SHA"

    # build is not on a branch, so name it with tag and tag-distance 
    elif [ -n "$REV_TAGROOT" ] && [ 0$REV_TAGDIST -gt 0 ]; then
        BRANCH="$REV_TAGROOT"
        LONG_BRANCH="tags/$BRANCH"
        REV="+$REV_TAGDIST"
        PKG_REV="${pkgtime_name:3}.tag.$SHA"
    fi

    [ -z "$BRANCH" ] && die "ref is unclassifiable: $ref with GIT_DIR=$GIT_DIR";

    # check if not using same location as head? 
    if [ "$(git rev-parse $ref)" != "$(git rev-parse HEAD)" ]; then
	:
    # check if zero diff found in files anywhere?
    elif GIT_WORKING_DIR=${src_root} git diff --ignore-submodules=all --quiet &&
	  GIT_WORKING_DIR=${src_root} git diff --cached --ignore-submodules=all --quiet; then
	:
    # else .. this working dir is not committed as is...
    else
        REV="${REV}.edit" # unversioned changes should be noted
        REV="${REV#.}" # remove a leading .
    fi

    # default branch name
    BRANCH="${BRANCH:-git}"

    BRANCH="${BRANCH//-/.}"  # remove - for branch name (not allowed)
    BRANCH="${BRANCH//_/.}"  # remove _ for branch name (not allowed)
}

branch_version_name() {
    local v="$1"

    # strip any path and remove trailing text from 'git describe'
    v=$(sed <<<"${v##*/}" -r -e 's,-[0-9]+-g[a-f0-9]+$,,')
    # change _ or . to an unused char '^' 
    v=$(sed <<<"$v" -r -e 's|[_.]+|^|g')
    # remove text before version number (numerals before a ^ char)
    v=$(sed <<<"$v" -r -e 's/^[^0-9]*([0-9]+\^)/\1/')
    # apply ^ replacement with . [numbers on both sides] repeatedly until none more found
    v=$(sed <<<"$v" -r -e ':start; s/([0-9])+\^([0-9])+/\1.\2/; tstart' | sed -e 'y,^,.,')
    echo $v
}

old_set_pkg_rev() {
###########################################
    # special branches used to exist for each variant
###########################################
    #local flavors="deb..\|mac..\|nexenta..\|rpm..\|sun.."
    #local qa_rc="qa..\|rc.."
    # Check if any known package flavors are found in the variable $BRANCH.
    # If found, remove from $VERSION and set $PKG_REV
    #PKG_REV=
    #if grep -E -q "$flavors" <<<"$BRANCH"; then
    #	PKG_REV=$(sed <<<"$BRANCH" -e "s/.*\($flavors\)/\1/")
    #fi
    # Also check for qa## or rc## and set PKG_REV, but don't strip.
    #if grep -E -q "$qa_rc" <<<"$BRANCH"; then
    #	PKG_REV=$(sed <<<"$BRANCH" -e "s/.*\($qa_rc\)/\1/")
    #fi
###########################################

    # Finally set a default.
    [ -z "$PKG_REV" ] && PKG_REV=1

    [[ "$VERSION" == *.git.* ]] && PKG_REV="0$(get_yearly_tag $pkg_name_pkgtime)"

    echo "Final PKG_REV value: $PKG_REV"
    # Write the file.
    echo "SET_PKG_REV : $PKG_REV"
    printf $PKG_REV > PKG_REV
}

save_version() {
    ref=$1
    # quiet!  no output until end
    get_git_info $ref >/dev/null

    VERSION=$(branch_version_name "$BRANCH")
    [ "${BRANCH}" = "trunk" ] && VERSION="trunk"
    VERSION+="${REV}"
    # old_set_pkg_rev o>/dev/null

    tmp=$(mktemp -d)

    # if we deduced the package name ... create a VERSION-TAR file
    if [ -n "$pkg_name" ]; then
        PKG_NAME_VER="${pkg_name}-$VERSION"
        repo_vers_dir="$tmp/${PKG_NAME_VER}"
        VERSION_TAR="$tmp/${PKG_NAME_VER}-versioning.tar"

        rm -rf $repo_vers_dir

        mkdir -p $repo_vers_dir

        echo -n $VERSION > $repo_vers_dir/FULL_VERSION
        echo -n "${PKG_REV:-1}" > $repo_vers_dir/PKG_REV
        echo -n $LONG_BRANCH > $repo_vers_dir/LONG_BRANCH
        echo -n $REV > $repo_vers_dir/REV
        tar -cf $VERSION_TAR -C $tmp ${PKG_NAME_VER}/.   # *keep* the /.
        rm -rf $repo_vers_dir
    fi

    # NOTE: using {} as a sub-scope is broken in earlier bash
    declare -p VERSION PKG_REV PKG_NAME_VER VERSION_TAR | 
	sed -e 's,^declare --,declare -g,';
    declare -p LONG_BRANCH BRANCH REV | 
	sed -e 's,^declare --,declare -g,';
    echo "echo \"setup version: $PKG_NAME_VER --- $PKG_REV\" "
}

# Fall back to previous build (or dist build?)?
if ! [ -d .git ]; then
    [ -f FULL_VERSION -a -f PKG_REV ] && exit 0
    echo "Error: No subversion or git info available!"   #### ERROR
    exit 1
fi

git remote -v show > vcs_repo.info
git --no-pager log --max-count=1 >> vcs_repo.info
save_version ${1:-HEAD}
#!/usr/bin/env bash
#
# This script is run by extension's autogen and amanda enterprise's builddist
# to create the FULL_VERSION and PKG_REV files.  Amanda Enterprise ignores the
# contents of VERSION because it comes from community and is static.  Instead
# Amanda Enterprise uses git branch or tag names.  Amanda Enterprise
# Extensions does not have a VERSION file.
#
# If run from a tag, the value of VERSION is the branch or tag name
# reformatted.
#
# Extensions ./configure uses FULL_VERSION in the macro ZAMANDA_VERSION (from
# config/zmanda_version.m4) to avoid hard coding the a version string.
# Amanda Enterprise ./configure has an equivalent macro AMANDA_INIT_VERSION
#set -x

if [ "$(type -t get_yearly_tag)" != "function" ]; then
    . packaging/common_z/build_functions.sh
fi

[ -x "$(command -v gsed)" ] && eval "sed() { command gsed \"\$@\"; }"
[ -x "$(command -v gmake)" ] && eval "make() { command gmake \"\$@\"; }"
[ -x "$(command -v ggrep)" ] && eval "grep() { command ggrep \"\$@\"; }"

sed --version | grep -i 'GNU sed' >/dev/null || exit 1;

detect_git_cache() {
    local url
    local name
    local repo
    local nrepo
    local gitdir=$(git rev-parse --git-dir):

    url=$1
    name=$url
    name=${name%.git};
    name=${name##*/};

    [ "$url" -ef "$(git rev-parse --show-toplevel)" ] && {
        # no cache exists.. use local
        return
    }

    [ -n "$name" ] || return 0

    repo=/opt/src/$name/.git
    [ -d "$repo" ] || return 0
    [ ! -s "$repo/shallow" ] || return 0

    nrepo="$(GIT_DIR=$repo git ls-remote --get-url origin)"
    nrepo="${nrepo##*/}"
    nrepo="${nrepo%.git}"

    [ "$nrepo" = "$name" ] || return 0
    [ "$repo/FETCH_HEAD" -ot "$gitdir/FETCH_HEAD" ] &&
       { GIT_DIR=$repo git fetch --prune origin || return 0; }
    # if nothing happened...
    [ "$repo/FETCH_HEAD" -ot "$gitdir/FETCH_HEAD" ] &&
        touch "$repo/FETCH_HEAD"
    echo $repo
}

get_latest_git_tag() {
    local ref="$1"
    local sha="$(git 2>/dev/null rev-parse --verify $ref)"
    local tags="$(git show-ref --tags | sed -e "\|^$sha |!d" -e 's,.* refs/tags/,,')"
    [ -n "$tags" ] || return;
    {
       # prioritize fully-numeric versions of latest numeric order by decimal
       grep    '[0-9.][^0-9.]' <<<"$tags" | sort -nt.
       grep -v '[0-9.][^0-9.]' <<<"$tags" | sort -nt.
    } | tail -1
}

get_git_info() {
    local ref
    local rmtref
    local repo
    local oref
    local cache_repo
    local pkgtime_name

    ref=$1
    git --no-pager log $ref --max-count=1 > vcs_repo.info

    # reduce the unix date of pkging dir to Jan 1st
    # ... and give an alpha code [<VOWELS>consonants>] to encode the minute
    pkgtime_name=$(get_yearly_tag $pkg_name_pkgtime)

    #default branch name

    # must be able to describe this... (using remote name!)
    rmtref="$(git describe --all --match '*/*' --always --exclude '*/HEAD' $ref 2>/dev/null)"
    rmtref="${rmtref:-$(git describe --all --match '*/*' --always $ref)}"

    # can't use HEAD so try to salvage it...
    [[ $rmtref == */HEAD ]] && rmtref="${rmtref%/*}/${ref##*/}"

    if [[ $rmtref == */* ]]; then
	# lose the exact branch name but get remote name
	rmtref=${rmtref#refs/}
	rmtref=${rmtref#remotes/}
	repo=$(git ls-remote --get-url "${rmtref%/*}");
    else
	repo=$(git ls-remote --get-url origin);
        rmtref=  # signal that nothing is a remote ref
    fi

    oref=
    cache_repo=
    [ -n "$repo" ] && cache_repo=$(detect_git_cache $repo) || true

    ############################################################
    #### MAY USE GIT_DIR ENVIRONMENT OVERRIDE #####
    if [ -d "$cache_repo" -a -n "$rmtref" ]; then
       oref="origin/${rmtref##*/}" || true
       rmtref="$oref"
       export GIT_DIR=$cache_repo
    fi

    if [ -z "$oref" -a -n "$rmtref" ]; then
        oref="$rmtref"
    elif [ -z "$oref" ]; then
        oref=$ref
    fi

    [ $oref = "origin/HEAD" ] && oref=$ref

    if [ -s $(git rev-parse --git-dir)/shallow ]; then
        ( set -xv; git fetch --unshallow; )  # must be done.. even if slow
    fi

    # keep the package-time in front to alphabetize-over-time
    local year_code=$(printf %02d $(( ${pkgtime_name:0:2} + 0 )) )
    year_code=${year_code:-$(date +%y)}
    declare -g SHA=$(git rev-parse --short $oref)
    local year_break

    local REF_IDEAL

    local REV_TAGPOS
    local REV_TAGDIST
    local REV_TAGROOT

    local REV_PLACE
    local REV_REFBR
    local REV_REFDIST

    # get name-rev version of tag-based names.. but snip any '^0' at end...
    ####### find tag-based [forward-relative] info for version
    local tmpbranch="$(git name-rev --name-only --refs='__temp_merge-*' $oref)"
    local tmpsubject="$(git log -1 --pretty=%s $oref)"
    local newsha="$(git rev-parse --verify --short ${oref}^2)"
    if [ -n "$newsha" ] && 
        [[ "$tmpbranch" == __temp_merge-* ]] && 
        [[ "$tmpsubject" == Merge\ remote-tracking\ branch*into\ $tmpbranch ]]; 
    then
        SHA="$newsha"
        t=$(git log --pretty='%at' -1 $SHA)
        t=$(( t + 0 ))
        [ $t -gt $pkg_name_pkgtime ] && pkg_name_pkgtime=$t
        pkgtime_name=$(get_yearly_tag $pkg_name_pkgtime)
        # get describe-based forward-aimed tag-name (root) and distance (dist) if that works
        REV_TAGPOS=${REV_TAGPOS:-$(get_latest_git_tag ${oref}^)}
        REV_TAGROOT=$(git describe --tags ${oref}^ 2>/dev/null)
    else
        REV_TAGPOS=${REV_TAGPOS:-$(get_latest_git_tag $oref)}
        REV_TAGROOT=$(git describe --tags $oref 2>/dev/null)
        # get describe-based forward-aimed tag-name (root) and distance (dist) if that works
    fi

    REV_TAGPOS="${REV_TAGPOS%^[0-9]*}"
    REV_TAGDIST="$(sed <<<"$REV_TAGROOT" -r -e 's/^.*-([0-9]+)-g[a-f0-9]+$/\1/' -e '/[^0-9]/d')"
    # trim off tag-dist
    [ -n "$REV_TAGDIST" ] && REV_TAGROOT="${REV_TAGROOT%-$REV_TAGDIST-g[a-f0-9]*}"

    # get name-rev branch-based name and distance (and prefer certain branches if possible)
    REF_IDEAL=$(git name-rev --name-only --refs='origin/next*[0-9].[0-9]*' $oref)
    REF_IDEAL="${REF_IDEAL#undefined}"
    REF_IDEAL="${REF_IDEAL:-$(git name-rev --name-only --refs='origin/stable*[0-9].[0-9]*' $oref)}"
    REF_IDEAL="${REF_IDEAL#undefined}"
    REF_IDEAL="${REF_IDEAL:-$(git name-rev --name-only --refs='origin/integ*[0-9].[0-9]*' $oref)}"
    REF_IDEAL="${REF_IDEAL#undefined}"
    REF_IDEAL="${REF_IDEAL:-$(git name-rev --name-only --refs='origin/dev*[0-9].[0-9]*' $oref)}"
    REF_IDEAL="${REF_IDEAL#undefined}"
    REF_IDEAL="${REF_IDEAL:-$(git name-rev --name-only --exclude=HEAD --refs='origin/*[0-9].[0-9]*' $oref 2>/dev/null)}"
    REF_IDEAL="${REF_IDEAL:-$(git name-rev --name-only --refs='origin/*[0-9].[0-9]*' $oref)}"
    REF_IDEAL="${REF_IDEAL#undefined}"

    REF_IDEAL="${REF_IDEAL%^0}"

    # get branch name separated out and get distance-back from branch (or zero)
    REV_REFBR=$(sed <<<"$REF_IDEAL" -r -e 's/\~[0-9~^]*$//')
    REV_REFDIST=$(sed <<<"$REF_IDEAL" -r -e 's/^[^~]*\~([0-9]+).*$/\1/' -e '/[^0-9]/s,.*,0,g' )
    REV_REFDIST=$(printf %02d $(( REV_REFDIST + 0 )))

    if [ -n "$REV_REFBR" -a $(( ${pkgtime_name:0:2} + 2000 )) -gt 2000 ]; then
        year_break=$(printf %04d-01-01 $(( ${pkgtime_name:0:2} + 2000 )) )
        read REV_YRDIST < <(git log --pretty='%H' --until $year_break $REV_REFBR | 
                             git name-rev --name-only --refs=$REV_REFBR --stdin | 
                             sed -r -e '/^[^~]*\~[0-9]+$/!d' -e q)
        REV_YRDIST=${REV_YRDIST##*~}
        REV_YRDIST=${REV_YRDIST%%^*}  # clean up if needed
        REV_YRDIST=${REV_YRDIST//[^0-9]}
        REV_YRDIST=$(printf %04d $(( REV_YRDIST - REV_REFDIST )))
        REV_YRDIST=.y${year_code}+${REV_YRDIST%%^*}  # clean up if needed
    fi
    
    ####### officially prove a tag or branch are reachable as a base

    # test if we have a specific (master) branch
    REV_PLACE="$(git rev-parse --short --symbolic-full-name "$REV_TAGPOS" 2>/dev/null)"   # get short hash-version
    REV_PLACE=${REV_PLACE:-"$(git rev-parse --short --symbolic-full-name "$REV_REFBR" 2>/dev/null)"}

    unset GIT_DIR
    #### END OF GIT_DIR ENVIRONMENT OVERRIDE (IF PRESENT) #####
    ############################################################

    # build is tagged precisely..
    if [ -n "$REV_TAGPOS" -a -z "$REV_TAGDIST" ]; then
        BRANCH="$REV_TAGPOS"
        LONG_BRANCH="tags/$BRANCH"
        REV=""       # no extra to branch info
        PKG_REV="1"  # precise name for it.. use no hash

    # build is simply from master
    elif [ "$REV_PLACE" = "refs/heads/master" ]; then
        BRANCH="trunk"
        LONG_BRANCH="${SHA}.trunk"
        REV="$REV_YRDIST"        # add commit# for year
        PKG_REV="${pkgtime_name:3}.git.$SHA"

    # build is the tip of a branch.. so name it with tag-distance if needed
    elif [ -n "$REV_REFBR" -a $((REV_REFDIST)) = 0 ]; then
        BRANCH="${REV_REFBR##*/}"
        LONG_BRANCH="branches/$BRANCH"
        REV="$REV_YRDIST"         # add commit# for year
        PKG_REV="${pkgtime_name:3}.git.$SHA"

    # build is not on a branch, so name it with tag and tag-distance 
    elif [ -n "$REV_TAGROOT" ] && [ 0$REV_TAGDIST -gt 0 ]; then
        BRANCH="$REV_TAGROOT"
        LONG_BRANCH="tags/$BRANCH"
        REV="+$REV_TAGDIST"
        PKG_REV="${pkgtime_name:3}.tag.$SHA"
    fi

    [ -z "$BRANCH" ] && die "ref is unclassifiable: $ref with GIT_DIR=$GIT_DIR";

    # check if not using same location as head? 
    if [ "$(git rev-parse $ref)" != "$(git rev-parse HEAD)" ]; then
	:
    # check if zero diff found in files anywhere?
    elif GIT_WORKING_DIR=${src_root} git diff --ignore-submodules=all --quiet &&
	  GIT_WORKING_DIR=${src_root} git diff --cached --ignore-submodules=all --quiet; then
	:
    # else .. this working dir is not committed as is...
    else
        REV="${REV}.edit" # unversioned changes should be noted
        REV="${REV#.}" # remove a leading .
    fi

    # default branch name
    BRANCH="${BRANCH:-git}"

    BRANCH="${BRANCH//-/.}"  # remove - for branch name (not allowed)
    BRANCH="${BRANCH//_/.}"  # remove _ for branch name (not allowed)
}

branch_version_name() {
    local v="$1"

    # strip any path and remove trailing text from 'git describe'
    v=$(sed <<<"${v##*/}" -r -e 's,-[0-9]+-g[a-f0-9]+$,,')
    # change _ or . to an unused char '^' 
    v=$(sed <<<"$v" -r -e 's|[_.]+|^|g')
    # remove text before version number (numerals before a ^ char)
    v=$(sed <<<"$v" -r -e 's/^[^0-9]*([0-9]+\^)/\1/')
    # apply ^ replacement with . [numbers on both sides] repeatedly until none more found
    v=$(sed <<<"$v" -r -e ':start; s/([0-9])+\^([0-9])+/\1.\2/; tstart' | sed -e 'y,^,.,')
    echo $v
}

set_build_version() {
    declare -g BUILD_VERSION

    read pkgbr < <(git config --blob ${SHA}:.gitmodules --get submodule.packaging.branch)
    read toolbr < <(git config --blob ${SHA}:.gitmodules --get submodule.tools-archives.branch)

    read pkgbr < <(git --git-dir=${src_root}/.git/modules/packaging rev-parse --verify --short origin/$pkgbr)
    read toolbr < <(git --git-dir=${src_root}/.git/modules/tools-archives rev-parse --verify --short origin/$toolbr)
    BUILD_VERSION="${pkgbr}.${toolbr}"
}

old_set_pkg_rev() {
###########################################
    # special branches used to exist for each variant
###########################################
    #local flavors="deb..\|mac..\|nexenta..\|rpm..\|sun.."
    #local qa_rc="qa..\|rc.."
    # Check if any known package flavors are found in the variable $BRANCH.
    # If found, remove from $VERSION and set $PKG_REV
    #PKG_REV=
    #if grep -E -q "$flavors" <<<"$BRANCH"; then
    #	PKG_REV=$(sed <<<"$BRANCH" -e "s/.*\($flavors\)/\1/")
    #fi
    # Also check for qa## or rc## and set PKG_REV, but don't strip.
    #if grep -E -q "$qa_rc" <<<"$BRANCH"; then
    #	PKG_REV=$(sed <<<"$BRANCH" -e "s/.*\($qa_rc\)/\1/")
    #fi
###########################################

    # Finally set a default.
    [ -z "$PKG_REV" ] && PKG_REV=1

    [[ "$VERSION" == *.git.* ]] && PKG_REV="0$(get_yearly_tag $pkg_name_pkgtime)"

    echo "Final PKG_REV value: $PKG_REV"
    # Write the file.
    echo "SET_PKG_REV : $PKG_REV"
    printf $PKG_REV > PKG_REV
}

save_version() {
    ref=$1
    # quiet!  no output until end
    get_git_info $ref >/dev/null
    set_build_version >/dev/null

    VERSION=$(branch_version_name "$BRANCH")
    [ "${BRANCH}" = "trunk" ] && VERSION="trunk"
    VERSION+="${REV}"
    # old_set_pkg_rev o>/dev/null

    tmp=$(mktemp -d)

    # if we deduced the package name ... create a VERSION-TAR file
    if [ -n "$pkg_name" ]; then
        PKG_NAME_VER="${pkg_name}-$VERSION"
        repo_vers_dir="$tmp/${PKG_NAME_VER}"
        VERSION_TAR="$tmp/${PKG_NAME_VER}-versioning.tar"

        rm -rf $repo_vers_dir

        mkdir -p $repo_vers_dir

        echo -n $VERSION > $repo_vers_dir/FULL_VERSION
        echo -n $BUILD_VERSION > $repo_vers_dir/BUILD_VERSION
        echo -n "${PKG_REV:-1}" > $repo_vers_dir/PKG_REV
        echo -n $LONG_BRANCH > $repo_vers_dir/LONG_BRANCH
        echo -n $REV > $repo_vers_dir/REV
        tar -cf $VERSION_TAR -C $tmp ${PKG_NAME_VER}/.   # *keep* the /.
        rm -rf $repo_vers_dir
    fi

    # NOTE: using {} as a sub-scope is broken in earlier bash
    declare -p VERSION BUILD_VERSION PKG_REV PKG_NAME_VER VERSION_TAR | 
	sed -e 's,^declare --,declare -g,';
    declare -p LONG_BRANCH BRANCH REV | 
	sed -e 's,^declare --,declare -g,';
    echo "echo \"setup version: $PKG_NAME_VER --- $PKG_REV\" "
}

# Fall back to previous build (or dist build?)?
if ! [ -d .git ]; then
    [ -f FULL_VERSION -a -f PKG_REV ] && exit 0
    echo "Error: No subversion or git info available!"   #### ERROR
    exit 1
fi

git remote -v show > vcs_repo.info
git --no-pager log --max-count=1 >> vcs_repo.info
save_version ${1:-HEAD}

