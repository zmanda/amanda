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
    . $(dirname ${BASH_SOURCE[0]})/build_functions.sh
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
    local tags="$(git show-ref --dereference --tags | sed -e "\|^$sha |!d" -e 's,.* refs/tags/,,' -e 's,\^{}$,,')"
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

    local year_code
    local tmpbranch
    local tmpsubject
    local newsha

    ref=$1
    while true; do
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
            if command git --git-dir=$cache_repo rev-parse --verify --quiet "origin/${rmtref##*/}"; then
               oref="origin/${rmtref##*/}"
               rmtref="$oref"
               export GIT_DIR=$cache_repo
            fi
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
        year_code=$(printf %02d $(( ${pkgtime_name:0:2} + 0 )) )
        year_code=${year_code:-$(date +%y)}

        # get name-rev version of tag-based names.. but snip any '^0' at end...
        ####### find tag-based [forward-relative] info for version
        tmpbranch="$(git name-rev --name-only --refs='__temp_merge-*' $oref)"
        [[ "$tmpbranch" != __temp_merge-* ]] && break

        tmpsubject="$(git log -1 --pretty=%s $oref)"
        [[ "$tmpsubject" != Merge\ remote-tracking\ branch*into\ $tmpbranch ]] && break

        newsha="$(git rev-parse 2>/dev/null --verify --short=9 ${oref}^2)"
        [ -z "$newsha" ] && break

        ref=$newsha
    done

    declare -g SHA=$(git rev-parse --short=9 $oref)

    local year_break

    local REF_IDEAL

    local REV_TAGPOS
    local REV_TAGDIST
    local REV_TAGROOT

    local REV_PLACE
    local REV_REFBR
    local REV_REFDIST

    # exact matches...
    REV_TAGPOS=${REV_TAGPOS:-$(get_latest_git_tag $oref)}
    REV_TAGROOT=$(git describe --tags $oref 2>/dev/null)
    REV_TAGPOS="${REV_TAGPOS%^[0-9]*}"
    REV_TAGDIST="$(sed <<<"$REV_TAGROOT" -r -e 's/^.*-([0-9]+)-g[a-f0-9]+$/\1/' -e '/[^0-9]/d')"
    # trim off tag-dist
    [ -n "$REV_TAGDIST" ] && REV_TAGROOT="${REV_TAGROOT%-$REV_TAGDIST-g[a-f0-9]*}"

    # prefer certain branches if multiples match
    # get describe branch-matching name
    REF_IDEAL="${REF_IDEAL:-$(git 2>/dev/null describe --all --exact-match --match 'origin/R-[Rr]elease*[0-9].[0-9]*' $oref)}" #1
    REF_IDEAL="${REF_IDEAL:-$(git 2>/dev/null describe --all --exact-match --match 'origin/[FH]-*[0-9].[0-9]*' $oref)}" #2
    REF_IDEAL="${REF_IDEAL:-$(git 2>/dev/null describe --all --exact-match --match 'origin/[D]-*[0-9].[0-9]*' $oref)}" #2
    REF_IDEAL="${REF_IDEAL:-$(git 2>/dev/null describe --all --exact-match --match 'origin/[a-z]*[a-z]-[0-9].[0-9]*' $oref)}" #3
    REF_IDEAL="${REF_IDEAL:-$(git 2>/dev/null describe --all --exact-match --match 'origin/*[0-9].[0-9]*' $oref)}" #5
    REF_IDEAL="${REF_IDEAL%^0}"

    # get branch name separated out and get distance-back from branch (or zero)
    REV_REFBR=$(sed <<<"$REF_IDEAL" -r -e 's/\~[0-9~^]*$//')
    REV_REFDIST=$(sed <<<"$REF_IDEAL" -r -e 's/^[^~]*\~([0-9]+).*$/\1/' -e '/[^0-9]/s,.*,0,g' )
    REV_REFDIST=$(printf %02d $(( REV_REFDIST + 0 )))

    if [ -n "$REV_REFBR" -a $(( ${pkgtime_name:0:2} + 2000 )) -gt 2000 ]; then
        year_break=$(printf %04d-01-01 $(( ${pkgtime_name:0:2} + 2000 )) )
        read REV_YRDIST < <(TZ=GMT git log --pretty='%H' --until $year_break $REV_REFBR |
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
    REV_PLACE="$(git rev-parse --short=9 --symbolic-full-name "$REV_TAGPOS" 2>/dev/null)"   # get short hash-version
    REV_PLACE=${REV_PLACE:-"$(git rev-parse --short=9 --symbolic-full-name "$REV_REFBR" 2>/dev/null)"}

    unset GIT_DIR
    #### END OF GIT_DIR ENVIRONMENT OVERRIDE (IF PRESENT) #####
    ############################################################
    # check if not using same location as head?
    REV_SUFFIX=

    # in case this is the only difference...
    # GIT_WORKING_DIR=${src_root} git reset ${src_root}/.gitmodules
    # GIT_WORKING_DIR=${src_root} git checkout ${src_root}/.gitmodules

    if [ "$(git rev-parse $ref)" != "$(git rev-parse HEAD)" ]; then
	:
    # check if zero diff found in files anywhere?
    elif GIT_WORKING_DIR=${src_root} git diff --ignore-submodules=all --quiet &&
	  GIT_WORKING_DIR=${src_root} git diff --cached --ignore-submodules=all --quiet; then
        :
    # else .. this working dir is not committed as is...
    else
        pkgtime_name=$(get_yearly_tag $(date +%s))
        REV_SUFFIX=".edit" # unversioned changes should be noted
    fi

    # build is tagged precisely..
    if [ -n "$REV_TAGPOS" -a -z "$REV_TAGDIST" ]; then
        BRANCH="$REV_TAGPOS"
        LONG_BRANCH="tags/$BRANCH"
        REV=""       # no extra to branch info
        PKG_REV="dev"  # precise name for it.. use no hash
        [ "${BUILD_FLAG}" = "release" ] && PKG_REV=1

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

    REV+=$REV_SUFFIX

    [ -z "$BRANCH" ] && die "ref is unclassifiable: $ref with GIT_DIR=$GIT_DIR";

    # default branch name
    BRANCH="${BRANCH:-git}"
}

branch_version_name() {
    local br="$1"
    local pre
    local post
    local ver

    # strip any path 
    br="${br##*/}"
    # remove overspecific text from 'git describe'
    br=$(sed <<<"$br" -r -e 's,-[0-9]+-g[a-f0-9]+$,,')
    # change - to _ until _ is changed to .
    # measure text before version number (last number)
    pre="${br%%[0-9]*}";
    # measure text after version number (first after number-or-dot)
    post="${br#*[0-9]}-"; post="${post#*[^0-9.]}"; 
    post="${br:$(( ${#br} - ${#post}))}"

    # if no version numbers available reject this name ...
    if [ $pre = $post ]; then return; fi            ###### RETURN

    # apply _ replacement with . [numbers on both sides] repeatedly until none more found
    [ -n "${post}" ] && ver=${br:${#pre}:-${#post}}  # skip post
    [ -z "${post}" ] && ver=${br:${#pre}}            # all

    [[ "$pre" == [D]-??*- ]] && ver+=".${pre%-}"
    [[ "$pre" == [FH]-* ]] &&   ver+=".${pre:0:1}"
    [[ "$pre" == [a-z]*- ]] && [[ "$pre" != *-[^-]* ]] && ver+=".${pre:0:1}"

    ver+="${post,,}"
    if [ "$ver" != "${ver:0:31}" ]; then 
        ver="${ver:0:31}" 
        ver="${ver%[-_.]*}"
    fi
    ver="${ver//[-_]/.}"
    echo "${ver}"
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
        echo -n $BUILD_VERSION > $repo_vers_dir/BUILD_VERSION
        echo -n "${PKG_REV:-1}" > $repo_vers_dir/PKG_REV
        echo -n $LONG_BRANCH > $repo_vers_dir/LONG_BRANCH
        echo -n $REV > $repo_vers_dir/REV
        tar -cf $VERSION_TAR -C $tmp ${PKG_NAME_VER}/.   # *keep* the /.
        rm -rf $repo_vers_dir
    fi

    # NOTE: using {} as a sub-scope is broken in earlier bash
    [ -n "$VERSION_TAR" ] && declare -p VERSION_TAR |
	sed -e 's,^declare --,declare -g,';
    [ -n "$PKG_NAME_VER" ] && declare -p PKG_NAME_VER |
	sed -e 's,^declare --,declare -g,';
    declare -p VERSION BUILD_VERSION PKG_REV |
	sed -e 's,^declare --,declare -g,';
    declare -p LONG_BRANCH BRANCH REV |
	sed -e 's,^declare --,declare -g,';
    echo "echo \"setup version: $PKG_NAME_VER --- $PKG_REV\" "
}

# Fall back to previous build (or dist build?)?
if ! [ -e .git ]; then
    [ -f FULL_VERSION -a -f PKG_REV ] && exit 0
    echo "Error: $(pwd): No subversion or git info available!"   #### ERROR
    exit 1
fi

git remote -v show > vcs_repo.info
git --no-pager log --max-count=1 >> vcs_repo.info
save_version ${1:-HEAD}
