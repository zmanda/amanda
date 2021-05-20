#!/bin/bash
# $* == <tag-name> <dirs-with-rpm-stubs> ....
#
# FIXME: MUST quick-test manifest files (via python?)
# FIXME: change to handle manifests / rpm pkg-files via python
# FIXME: change to use scripts from manifests only [excepting debian preinst only]
#

#set -xv

selfpath=$(realpath $0)
# need the correct python as well as other scripts
export PATH=${selfpath%/*}:/opt/zmanda/amanda/bin:${PATH}
MANIFESTS_DIR="/opt/zmanda/amanda/pkg-manifests"

#       PREIN, POSTTRANS, PREUN, POSTUN, VERIFYSCRIPT

declare -a args
declare -a opts

# uppercase whatever is there!
tag="${1}"
[ x"$tag" = x'--test' ] && tag=TESTIN
tag="${tag^^}"

# leave it if its an -* arg
[[ $1 == -* ]] || shift

cmd_arg=

for cli do
    case "$cli" in
      # need to pass the alternate-version to debian-style scripts..
      --upgrade=?*)
         cmd="upgrade"
         cmd_arg="${cli#*=}"
         ;;

      -*) opts+=("$cli");;
      *) args+=("$cli");;
    esac
done

unset -f pkg_names pkg_unpkd_names pkg_script

manif_suffix=manifest
manif_suffix_pre=manifest.preun

if [ -x "$(command -v rpm)" ] && rpm -q -f /etc/os-release 2>/dev/null >&2; then
##########################################################################################################
    tag=${tag^^}

    function get_manifest() {
        local manif=
        local pkg=$1
        if [ -r "$pkg" ]; then
            manif="$(rpm -q --qf "$MANIFESTS_DIR/%{NAME}.${manif_suffix%.preun}" -p $pkg)"
            manif="${manif//_/-}"  # unify with debian names
            echo "$manif"
            return 0
        fi
        manif="$(rpm -q --qf "$MANIFESTS_DIR/%{NAME}.$manif_suffix" $pkg)"
        manif="${manif//_/-}"  # unify with debian names
        [ -r "$manif" ] || manif="${manif/%$manif_suffix/$manif_suffix_pre}"
        manif="${manif//_/-}"  # unify with debian names
        [ -r "$manif" ] && echo $manif
    }

    function pkg_names() {
        local p=
        [ $# = 0 ] && return
        find "$@" -maxdepth 1 -name '*.rpm' -type f |
          while read p; do
              rpm2cpio $p | grep -qam1 . && continue
              rpm -q --qf "%{NAME}\t$p\n" -p $p;
          done
    }

    function pkg_unpkd_names() {
        local manif=
        local pkgnm=

        # get a path and error messages if needed
        for pkgnm do
            rpm -q $pkgnm >/dev/null || continue;
            manif="$(get_manifest $pkgnm)"
	    [ -r $manif ] && rpm -q --qf "%{NAME}\t$manif\n" $pkgnm
        done
    }

    function pkg_script() {
        local tag=${1^^};
        local nm=$2;
        local rpmstub=
        local ver=
        local manif=

        [ -z "$nm" ] && return
        [ -z "$tag" ] && return

        [ -d $MANIFESTS_DIR ] || install -d -m 0555 $MANIFESTS_DIR

        rpmstub="${pkg_set[$nm]}"

        if [ x$tag = xPREIN ]; then
            # PREIN and POSTUN script only from an rpm file found...
            ver="$(rpm -q -p $rpmstub --qf '%{VERSION}-%{RELEASE}\n' 2>/dev/null)"
            manif="$(get_manifest $rpmstub)"
        else
            # possibly > 1 version
            ver="$(rpm -q $nm --qf '%{VERSION}-%{RELEASE}\n' 2>/dev/null)"
            # script only from the latest/earliest of installed pkgs
            # on install take 'latest' version every time for install...
            # on uninstall take 'earliest' version every time for uninstall...
            [[ "$tag" == *UN ]] || ver="$(sort -rt. <<<"$ver" | head -1)"
            [[ "$tag" == *UN ]] && ver="$(sort -t. <<<"$ver" | head -1)"
            manif="$(get_manifest $nm-$ver)"
        fi


        #echo "set -xv;"

        case ${tag} in
            #
            # for RPM simulate a DEB 'unpack':
            #    - run the PREIN   (no new manifest saved at all!)
            #    - install new manifest stub package state (from stub)
            #    - replace with new manifest file (rename .preun if needed)
            #    - clear any symlinks and do stub-only setup
            #    - (fail with exit if needed)
            PREIN)
                echo "echo '#### pkg_script: start $tag $nm'"
                rpm -q -p $rpmstub --qf "%{${tag}}\n"
                echo "echo '#### pkg_script: unpack $tag $nm'"
                echo "set -e"
                echo "rpm --justdb -U --quiet --nodigest --noscripts --ignoreos \"${rpmstub}\" 2>&1"
                echo "install -S .preun -D -m 444 \"${rpmstub}\" \"${manif}\""
                echo "echo '#### pkg_script: restored-clred $tag $nm'"
                echo "( set -x; ${selfpath%/*}/restore-pkg-perms --clr-symlinks --conf-save $nm; )";
                echo "echo '#### pkg_script: end $tag $nm'"
            ;;

            # for RPM simulate a DEB 'configure' state:
            #    - fix Bitrock problems with installed system, missing dirs, missing symlinks
            #    - run POSTTRANS
            #    - (fail with exit if needed)
            POSTTRANS)
                echo "echo '#### pkg_script: start $tag $nm'"
                echo "set -e"
                echo "${selfpath%/*}/restore-pkg-perms $nm"
                echo "set +e"
                echo "echo '#### pkg_script: run-script $tag $nm'"
                rpm -q $nm --qf "%{${tag}}\n"
                echo "echo '#### pkg_script: end $tag $nm'"
            ;;

            # may be an abort ...
            #    - pre-move any manifest if available
            #    - pre-copy any manifest to .preun
            #    - run PREUN script
            #    - (fail with exit if needed)
            PREUN)
                rpm -q --whatrequires $nm | cat -n >&2
                # success means it is required
                if [[ $nm == *shared* ]]; then
                    echo "#### pkg_script: pre-un not-allowed-here $tag $nm";
                    echo "exit 0";
                    return 0;
                fi

                # allow for upgrades by renaming
                [ "$rpmstub" -ef "${manif%.preun}" ] && mv -f "${manif%.preun}" "${manif}"
                # moved in line above?  next line is skipped .. else make an install-copy
                [ -r "${rpmstub}" ] && install -m 444 "${rpmstub}" "${manif}"
                echo "echo '#### pkg_script: start $tag $nm'"
                rpm -q $nm --qf "%{${tag}}\n"
                # save every config file left in-place ... before bitrock erases
                echo "( set -x; ${selfpath%/*}/restore-pkg-perms --conf-save $nm )";
                echo "echo '#### pkg_script: end $tag $nm'"
            ;;

            # may be an abort ...
            #    - attempt to perform rpm -e if anything failed and NOT within an install
            #    - run POSTUN script
            #    - remove now-redundant manif.preun file and dir
            #    - (must succeed if passed thru)
            POSTUN)
                rpm -q --whatrequires $nm | cat -n >&2
                # success means it is required
                if [[ $nm == *shared* ]] &&
                    rpm -q --whatrequires $nm | xargs -rn1 rpm -q >/dev/null; 
                then
                    echo "#### pkg_script: remove-not-allowed $tag $nm";
                    echo "exit 0";
                    return 0;
                elif [[ $nm == *shared* ]]; then
                    [ "$rpmstub" -ef "${manif%.preun}" ] && mv -f "${manif%.preun}" "${manif}"
                    [ -r "${rpmstub}" ] && install -m 444 "${rpmstub}" "${manif}"
                    echo "echo '#### pkg_script: start PREUN $nm'"
                    rpm -q $nm --qf "%{PREUN}\n"
                    echo "( set -x; ${selfpath%/*}/restore-pkg-perms --conf-save $nm )";
                    echo "echo '#### pkg_script: end PREUN $nm'"
                fi

                echo "echo '#### pkg_script: start $tag $nm'"
                echo "rpm -e --quiet --noscripts \"$nm-$ver\" >&/dev/null || true"
                echo "echo '#### pkg_script: deinstalled $tag $nm'"
                rpm -q $nm --qf "%{${tag}}\n"
                echo "echo '#### pkg_script: POSTUN done $tag $nm'"

                echo "_${tag}_r=\$?"
                echo "echo '#### pkg_script: exit-script $tag $nm and ' \$_${tag}_r"
                echo "( set -x; rm -f ${manif} )"
                echo "rmdir -p ${manif%/*} || true"
                echo "echo '#### pkg_script: end $tag $nm and ' \$_${tag}_r"
                echo "exit \$_${tag}_r"
            ;;
        esac
    }

    function test_install() {
        local r=
        # unfortunately files are never treated correctly for upgrades
        (
            exec > >(awk >&2 ' /conflicts with file from package/ { if ( ++n < 5 ) print; next; }  { print; } ')
            rpm --justdb -U --test --quiet --nodigest --noscripts --ignoreos "$@" 2>&1
            r=$?
            return $r
        )
        return $?
    }
    type="RPM"

##########################################################################################################
elif [ -x "$(command -v dpkg-query)" ] && dpkg-query -L base-files 2>/dev/null | grep -q /etc/os-release; then
    function get_manifest() {
        local manif
        manif="$(dpkg-query -W --showformat "${MANIFESTS_DIR}/\${Package}.$manif_suffix" $1)"
        manif="${manif//_/-}"  # unify with debian names
        [ -r "$manif" ] || manif="${manif/%$manif_suffix/$manif_suffix_pre}"
        manif="${manif//_/-}"  # unify with debian names
        [ -r "$manif" ] && echo $manif
    }

    function pkg_names() {
       [ $# = 0 ] && return
       find "$@" -maxdepth 1 -name '*.deb' -type f |
          while read p; do
              [ -n "$p" ] || continue
              # { ar p $p data.tar.gz | tar -tzf - | grep -q var/lib/dpkg/info/stable-add; } || continue
              dpkg-deb -W --showformat "\$""{Package}\t$p\n" $p;
          done
    }

    function pkg_unpkd_names() {
        for pkgnm do
            [ -n "$pkgnm" ] || continue
            dpkg-query -W --showformat '$''{Status}' $pkgnm 2>/dev/null |
			grep -q -e 'ok installed' -e 'ok unpacked' ||
               continue
            manif="$(get_manifest $pkgnm)"
	    [ -r $manif ] && dpkg-query -W --showformat '$'"{Package}\t$manif\n" $pkgnm
        done
    }

    function pkg_script() {
        local debscript=$1;
        local nm=$2;
        [ -n "$nm" -a -n "$debscript" ] || return

        debstub="${pkg_set[$nm]}"
        manif="$(get_manifest $nm)"
        rpmstub="$manif"

        #echo "set -xv;"

        # add into script
        case $debscript in
            # preps a DEB 'unpack':
	    #    - move existing manifest to .preun to protect it
	    #    - dpkg-unpack
            #       * runs the preinst
            #       * upgrade/install the stub file (incomplete)
            #       * manifest is installed
            #    - clear any symlinks and do stub-only completion
            #    - (fail with exit if needed)
            preinst)
                # no manif exists at this point... cannot query
                echo "echo '#### pkg_script: start $debscript $nm'"
                echo "set -e"
                echo "( set -x; dpkg --force-confold --force-confmiss --unpack \"$debstub\" 2>&1; ) || exit \$?"
                echo "echo '#### pkg_script: restored-clred $tag $nm'"
                echo "( set -x; ${selfpath%/*}/restore-pkg-perms --clr-symlinks --conf-save $nm; )";
                echo "echo '#### pkg_script: end $debscript $nm'"
            ;;

            # preps a DEB 'configure':
            #    - fix Bitrock problems, missing dirs, missing symlinks
            #    - dpkg-configure
            #       * run postinst (and for any other needed packages too?)
            #    - (fail with exit if needed)
            postinst)
                echo "echo '#### pkg_script: start $debscript $nm'"
                echo "set -e"
                # this is to *repair* the stub and make things clean up
                echo "${selfpath%/*}/restore-pkg-perms $nm"
                echo "echo '#### pkg_script: restored $debscript $nm'"
                echo "( set -x; dpkg --configure $nm; )"
                echo "echo '#### pkg_script: end $debscript $nm'"
            ;;

            # may be an abort ...
            #    - pre-move any manifest if available
            #    - pre-copy any manifest to .preun
            #    - run prerm script
            #    - (fail with exit if needed)
            prerm)
                # dont run these scripts if reinstreq hold or hold-reinstreq are present... "
                eflag="$(dpkg-query 2>/dev/null -W --showformat '${db:Status-Eflag}' $nm)"
                state="$(dpkg-query 2>/dev/null -W --showformat '${db:Status-Status}' $nm)"
                # dont run these scripts if reinstreq hold or hold-reinstreq are present...
                [[ $nm == *shared* ]] && {
                    echo "#### pkg_script: script-not-allowed-yet $debscript $nm";
                    return 0;
                }
                [ "$eflag" = ok -a "$state" = installed ] || {
                    echo "#### pkg_script: script-badstatus $debscript $nm";
                    echo "( set -x; ${selfpath%/*}/restore-pkg-perms --conf-save $nm )";
                    echo "exit 102";
                    return 0;
                }
                # allow for upgrades by renaming
                [ "$rpmstub" -ef "${manif%.preun}" ] && mv -f "${manif%.preun}" "${manif}"
                # moved in line above?  next line is skipped .. else make an install-copy
                [ -r "${rpmstub}" ] && install -m 444 "${rpmstub}" "${manif}"
                echo "echo '#### pkg_script: start $debscript $nm'"
                # print out script .. and then truncate immediately...
                dpkg-query --control-show $nm $debscript 2>/dev/null && 
                   xargs -r tee <<<"$(dpkg-query --control-path $nm $debscript)"
                echo "###"     # need a \n because debian may not have ending one
                echo "echo '#### pkg_script: end $debscript $nm'"
                echo "( set -x; ${selfpath%/*}/restore-pkg-perms --conf-save $nm )";
                echo "echo '#### pkg_script: end $debscript $nm'"
            ;;

            # may be an abort ...
            #    - attempt to quit out...
            #    - run postrm script
            #    - remove now-redundant manif.preun file and dir
            #    - (must succeed if passed thru)
            postrm)
                # dont run these scripts if reinstreq hold or hold-reinstreq are present... "
                eflag="$(dpkg-query 2>/dev/null -W --showformat '${db:Status-Eflag}' $nm)"
                state="$(dpkg-query 2>/dev/null -W --showformat '${db:Status-Status}' $nm)"
                [ "$eflag" = ok -a "$state" = installed ] || {
                    echo "#### pkg_script: script-badstatus $debscript $nm";
                    echo "exit 104";
                    return 0;
                }
                echo "dpkg -r $nm >&/dev/null || true"
                echo "want=\$(dpkg-query 2>/dev/null -W --showformat '\${db:Status-Want}' $nm)"
                echo "state=\$(dpkg-query 2>/dev/null -W --showformat '\${db:Status-Status}' $nm)"
                # quit if did not remove it
                echo "[ X\$want = Xdeinstall -a X\$state = Xconfig-files ] ||"
                echo "[ X\$want = Xdeinstall -a X\$state = Xnot-installed ] ||"
                echo "   { dpkg --set-selections <<<'$nm install'; exit 0; }"
                echo "echo '#### pkg_script: exit-script $tag $nm'"
                echo "( set -x; rm -f ${manif} )"
                echo "rmdir -p ${manif%/*} || true"
                echo "echo '#### pkg_script: end $debscript $nm'"
            ;;
        esac
    }

    function test_install() {
        local debstubs=("$@")
        local aptver=
        local nms=()
        local out=
        local nmfilt=

        aptver="$(dpkg-query -W --showformat '$''{Version}' apt)"
        nms=($(for debstub in "${debstubs[@]}"; do dpkg-deb -W --showformat '$''{Package}\n' $debstub; done))
        # newer version....
        if [[ $aptver > 1.1 ]]; then
            out="$(apt-get install --no-upgrade --no-download --simulate "$@" 2>&1)"
	    r=$?
            sed <<<"$out" >&2 \
               -e '1,/^$/d' \
               -e '/\.\.\.$/d' \
               -e '/apt-get -f install/d' \
               -e '/^Inst /d' \
               -e '/^Conf /d'

            [ $((r)) = 0 ] || return $r
            [ "$(grep -c '^Inst ' <<<"$out")" = ${#nms[@]} ] || return 1
            [ "$(grep -c '^Conf ' <<<"$out")" = ${#nms[@]} ] || return 1
            grep <<<"$out" -q '^E:' && return 1
            return 0
        fi

        if [ -n "${#nms[*]}" ] && dpkg-query --showformat '$''{Status}\n' -W "${nms[@]}" 2>/dev/null | grep -q ' installed$'; then
           dpkg-query -l "${nms[@]}"
           return 1;
        fi
        # older messy version....
        nmfilt="$(IFS='|'; echo "${nms[*]}")"
        #
        # pipeline.. not for loop
        #
        { for debstub in "${debstubs[@]}"; do dpkg-deb -W --showformat '$''{Depends}\n' $debstub; done; } |
            sed -e 's/([^)]*)//g' -e 's/, */\n/g' |
            egrep -v "$nmfilt" | sort -u |
            xargs -n1 -t -r dpkg-query -W --showformat '$''{Status}\n' |
            grep -qv 'ok installed'
        # grep success needs to be failure...
        [ $? != 0 ]
    }

    # debian modes
    type="DEB"
    tag="${tag,,}"        # lowercase
    tag="${tag/un/rm}"    # chg un to rm
    tag="${tag/in/inst}"  # chg in to inst
    tag="${tag/posttrans/postinst}"  # chg in to inst

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
declare -A pkg_unpkd_cnt
declare -a testinstpkgs

[ $(id -u) = 0 ] || \
   die "Root permissions are missing"


# create a list of pkgs/files found for all these ops first...

case $tag in
     # need a literal list of files
     PREIN|preinst|TESTIN|testinst)
         list="$(pkg_names $(ls 2>/dev/null -1d "${args[@]}"))"
         ;;
     # need a set of package names
     POSTTRANS|postinst)
         list="$(pkg_unpkd_names "${args[@]}")"
         ;;
     PREUN|POSTUN|prerm|postrm)
         # prefer .preun manifest first if found ...
         manif_suffix=manifest.preun
         manif_suffix_pre=manifest
         list="$(pkg_unpkd_names "${args[@]}")"
         ;;
     *) cat >&2 <<USAGE
Usage: ${0##*/} PREIN <directory> <file>.rpm <file>.deb
Usage: ${0##*/} (PREUN|POSTTRANS|POSTUN)  <package-name> <package-name> <package-name>
USAGE
        exit 1;
esac

# default adjust is zero
# [ $tag = PREIN ] && adj=1

while read pkgnm pkgpath extra; do
    [ -n "$pkgnm" ] || continue
    pkg_unpkd_cnt[$pkgnm]=$(( $(pkg_unpkd_names $pkgnm | grep -c .) + adj ))
    pkg_set[$pkgnm]="$pkgpath"
done <<<"$list"

order_of_install=(\
   zmanda-platform-shared \
   amanda_enterprise-platform \
   aee-backup-platform \
   amanda_enterprise-backup-server \
   amanda_enterprise-extensions-server \
   zmanda-ui-platform \
   zmc-ui-platform \
   zmc-ui-nodejs \
   aee-backup-server \
   zmc-ui-rest \
   )

if [[ $tag == *UN ]] || [[ $tag == *rm ]]; then
    rev=()
    for pkgnm in ${order_of_install[@]}; do
        rev=($pkgnm ${rev[@]})
    done
    order_of_install=(${rev[@]})
fi

[ $type = DEB ] && order_of_install=("${order_of_install[@]//_/-}")

#
# DETERMINE CORRECT DEBIAN-STYLE OPERATIONS FOR THIS RUN
#
case $tag in
   # just attempt installs on all at once
   TESTIN) cmd="test";;
   testinst) cmd="test";;

   # for the test installs, debian is okay with stopping upgrades ...
   # the total list of pre-installed pkgs should be shown all at once...
   TESTIN) cmd="test";;
   testinst) cmd="test";;

   # no install file but previously installed... is okay
   testinst) cmd="test";;
   # skip a present shared file in a test...
   testinst) cmd="";;

   # prep-unpack-only... (a stub install re-adds file list in debian)
   PREIN) cmd="install"; retrytag=POSTUN; retrycmd=abort-install;;
   preinst) cmd="install"; retrytag=postrm; retrycmd=abort-install;;

   # unpack-to-installed conversion
   POSTTRANS) cmd="configure";;   # no retry
   postinst) cmd="configure";; # no retry
   # to remove (without bitrock help) ...
   PREUN) cmd=${cmd:-remove}; retrytag=POSTTRANS; retrycmd=abort-remove;;
   prerm) cmd="remove";       retrytag=postinst; retrycmd=abort-remove;;

   # att
   POSTUN) cmd=${cmd:-remove}; debokay="purge";;
   postrm) cmd="remove"; debokay="purge";;

   *)
       echo "--------------------------------------- $tag ... failed tag-command check"
       die "ERROR: internal error with tag=$tag"                  ### EXIT ###
       ;;
esac


for pkg in "${order_of_install[@]}"; do
    [ -n "$pkg" ] || continue
    pkgpath=
    [ -r "${pkg_set[$pkg]}" ] && pkgpath="${pkg_set[$pkg]}"
    unpkdchk="${pkg_unpkd_cnt[$pkg]}"
    unpkdchk="${unpkdchk:-0}"

    case $cmd,$unpkdchk,$pkgpath in
       # just attempt installs on all at once

       # just skip over a present shared file in a test...
       test,1,*/zmanda-platform-shared.deb) pkgpath="";;
       test,1,*/zmanda-platform-shared.rpm) pkgpath="";;

       test,*,?*) ;;     # test with already-unpkdchk too

       install,0,?*) ;;  # install only without earlier pkg
       configure,1,*) ;; # configure only with earlier pkg

       remove,1,*) ;;  # some way needed to get script
       remove,0,?*) ;; # some way needed to get script

       upgrade,1,*) ;;  # some way needed to get script
       upgrade,0,?*) ;; # some way needed to get script

       #
       # FIXME: upgrades...
       #
       *,0,)
           echo "--------------------------------------- $pkg / $cmd" ;;
       *)
           echo "--------------------------------------- $pkg / $cmd ... failed default check"
           die "ERROR: internal error $cmd,$unpkdchk,$pkgpath"                  ### EXIT ###
           ;;
    esac

    [ -z "$pkgpath" ] && continue                                                ### CONTINUE ###

    if [ $cmd = test ];  then
        [ -n "$pkgpath" ] && testinstpkgs+=("$pkgpath")
        echo "--------------------------------------- $pkg / $tag ... added to test set"
        continue                                                             ### CONTINUE ###
    fi

    # install, or configure or and uninstall changes all run here
    echo "--------------------------------------- $pkg / $tag ... attempting $cmd script run";
    script="$( pkg_script $tag $pkg | grep -v '^(none)' )"
    sed -e "s,^,$pkg.$tag/$cmd:," <<<"$script" | cat -n
    bash -s -- $cmd $cmd_arg <<<"$script"
    r=$?
    [ $r = 0 ] && continue                                                   ### CONTINUE ###

    sleep 1
    echo "==================== WARNING: failed st=$r: cmd=$cmd $tag,$unpkdchk,$pkgpath"
    [ -z "$retrytag" ] && die "ERROR: no retry scripts to try"

    echo "==================== WARNING: attempting abort/retry: cmd=$retrytag $retrycmd $tag,$unpkdchk,$pkgpath"
    script="$( pkg_script $retrytag $pkg | grep -v '^(none)' )"
    sed -e "s,^,$pkg.$retrytag/$retrycmd:," <<<"$script" | cat -n
    bash -s -- $retrycmd $cmd_arg <<<"$script"
    rr=$?
    echo "==================== WARNING: retry done st=$?: cmd=$retrytag $retrycmd $tag,$unpkdchk,$pkgpath"
    [[ $retrycmd == abort* ]] && die "abort completed"
    exit $rr;
    exit $r                                               ### FAILURE ###
done

if [ ${#testinstpkgs[@]} -gt 0 ]; then
    result=success
    for pkgpath in "${testinstpkgs[@]}"; do
	nm=$(pkg_names $pkgpath)
	[ -n "$nm" ] || continue
	read nm path <<<"$nm"
        [ $((pkg_unpkd_cnt[$nm])) -gt 0 ] &&
           echo "============ INFO: already installed $nm" >&2
        [ $((pkg_unpkd_cnt[$nm])) -gt 0 ] &&
           result=upgrade
    done
    #
    # FIXME: must decide to block installs/upgrades here...
    #
    [ $result == success ] || die $'\n'"ERROR: failed test install: ${testinstpkgs[@]##*/}"
    test_install "${testinstpkgs[@]}" || die $'\n'"ERROR: failed test install: ${testinstpkgs[@]##*/}"
    # MUST PRINT CRITICAL RESPONSE TO SUCCESSFUL TEST...
    echo "--------------------- success ${args[@]}"
fi

true
