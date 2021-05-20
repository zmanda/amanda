#!/bin/bash
setopt=$-

alias run-pkg-scripts=/bin/true
if ! [ -x "$(command -v run-pkg-scripts)" ]; then
    x="$(command ls -1t /opt/zmanda/amanda/*/run-pkg-scripts 2>/dev/null | head -1)"
    [ -x "$x" ] && alias run-pkg-scripts="$(realpath $x)"
fi

#####################################################################
# pre-install script tests

rpm_bin=$(command -v rpm)
dpkgq_bin=$(command -v dpkg-query)

declare -a PKG_SCRIPT_ARGS=("${PRE_INST_ARGS[@]}" "${POST_INST_ARGS[@]}" "${PRE_RM_ARGS[@]}" "${POST_RM_ARGS[@]}")

pkgtype=UNKN
pkgscript=UNKN

if [ -s /etc/redhat-release -a -x "$rpm_bin" ]; then
    pkgtype=RPM
elif [ -s /etc/debian_version -a -x "$dpkgq_bin" ]; then
    pkgtype=DEB
fi

# only one arg, a number and system supports rpm
if [ $pkgtype = RPM \
       -a "${#PKG_SCRIPT_ARGS[@]}" = 1 \
       -a $((PKG_SCRIPT_ARGS[0])) = "${PKG_SCRIPT_ARGS[0]}" ]; then
    pkgscript=RPM
elif [ $pkgtype = DEB \
       -a "${#PKG_SCRIPT_ARGS[@]}" -ge 1 \
       -a $((PKG_SCRIPT_ARGS[0])) != "${PKG_SCRIPT_ARGS[0]}" ]; then
    pkgscript=DEB
fi

if [ ${#PRE_INST_ARGS[@]} -gt 0 ]; then
    unset have_old_config

    is_install_new() {
        chk="${PRE_INST_ARGS[0]},${PRE_INST_ARGS[1]}"
        case "$chk" in
           [01],)        # RPM no-pkgs clean (mostly) install
              return 0;;
           install,*)    # debian no-pkgs clean install (possibly *unpurged* config)
              return 0;;
           *) return 1;; # some sort of upgrade has started
        esac
    }

    is_reinstall_after_abort() {
        chk="${PRE_INST_ARGS[0]},${PRE_INST_ARGS[1]}"
        case "$chk" in
           # no way to tell for RPMs...
           abort*) # attempt to clean up prep after a failed old-package removal
              return 0;;
           *) return 1;;  # no attempt to reinstall found
        esac
    }

    have_old_config() {

        chk="${PRE_INST_ARGS[0]},${PRE_INST_ARGS[1]}"
        case "$chk" in
           # perform a test to see if nothing pre-exists
           # (apparently cannot trust second-arg to have nothing!)
           [01],)      ;&
           install,)
               find /etc/amanda/* /var/lib/amanda/* -type d -quit 2>/dev/null ||
                    return 1  # nothing found..
               return 0;;    # something found

           *)          # some config is likely...
              return 0;;
        esac
    }
       
    #
    # automatic upgrade handling for RPMs (multiples not allowed normally): 
    #   run PREUN for older version if possible
    #

    if [ $pkgtype = RPM ]; then
        oldver="$(rpm 2>/dev/null -q --qf "%{VERSION}\n" ${RPM_PACKAGE_NAME})"
        # skip exactly one if it matches our own version 
        oldver="$(awk "! /$RPM_PACKAGE_VERSION/ && cnt++ > 0 { print; } " <<<"$oldver")"
        if rpm -q "${RPM_PACKAGE_NAME}-$oldver" >/dev/null; then
            PRE_INST_ARGS=(upgrade $oldver)
            PKG_SCRIPT_ARGS=(${PRE_INST_ARGS[@]})
            echo "========================== (early PREUN) from [$RPM_PACKAGE_NAME-$RPM_PACKAGE_VERSION] args: ${PKG_SCRIPT_ARGS[@]} ${!PRE_*}${!POST_*} ============="
            run-pkg-scripts PREUN --upgrade=${RPM_PACKAGE_VERSION} ${RPM_PACKAGE_NAME}
            echo "========================== (PREUN return) from [$RPM_PACKAGE_NAME-$RPM_PACKAGE_VERSION] args: ${PKG_SCRIPT_ARGS[@]} ${!PRE_*}${!POST_*} ============="
        fi
    fi

    # fixup of context for DEBs during unpack or upgrade??
    #if [ $pkgtype = DEB -a -z "${PRE_INST_ARGS[1]}" ]; then
    #    oldver="$($dpkgq_bin 2>/dev/null -W --showformat '${VERSION}' ${RPM_PACKAGE_NAME//_/-})";
    #    PRE_INST_ARGS[1]=$oldver
    #fi
fi

#####################################################################
# post-install script tests

if [ ${#POST_INST_ARGS[@]} -gt 0 ]; then
    unset have_old_config
    unset is_install_new

    is_install_new() {
        chk="${POST_INST_ARGS[0]},${POST_INST_ARGS[1]}"
        case "$chk" in
           [01],)        # RPM no-pkgs clean (mostly) install
              return 0;;
           configure,)    # debian no-pkgs clean install (possible *unpurged* config)
              return 0;;
           *) return 1;; # some sort of upgrade has started
        esac
    }

    have_old_config() {
        chk="${POST_INST_ARGS[0]},${POST_INST_ARGS[1]}"
        case "$chk" in
           # perform a test to see if nothing pre-exists
           [01],) find /etc/amanda/* /var/log/amanda/*/* -type d -quit 2>/dev/null ||
                     return 0;  # success means config was found
               return 1;;   # failure means no old config was found
           configure,)        
              return 1;;    # no unpurged config will be left
           *)          
              return 0;;    # some config is likely for any upgrade
        esac
    }
fi

#####################################################################
# shared installing script tests

is_reinstall_after_abort() {
    chk="${PRE_INST_ARGS[0]}${POST_INST_ARGS[0]},${PRE_INST_ARGS[1]}${POST_INST_ARGS[1]}"
    case $chk in
       # no way to tell for RPMs...
       abort*) # attempt to clean up prep after a failed old-package removal
          return 0;;
       *) return 1;;
    esac
}


#####################################################################
# shared removal script tests

is_uninstall_after_abort() {
    chk="${POST_RM_ARGS[0]}${PRE_RM_ARGS[0]},${POST_RM_ARGS[1]}${PRE_RM_ARGS[1]}"
    case $chk in
       # no way to tell for RPMs...
       abort*)    # an attempt to fix prep after a failed old-package removal
          return 0;;
       failed*)   # an attempt to recover after failed remove
          return 1;; 
       *) return 1;;  # no attempt to reinstall found
    esac
}

#####################################################################
# shared removal script tests

is_full_remove() {
    chk="${POST_RM_ARGS[0]}${PRE_RM_ARGS[0]},${POST_RM_ARGS[1]}${PRE_RM_ARGS[1]}"
    case $chk in
       0,)        # RPM no-pkgs clean (mostly) install
          return 0;;
       remove,*)    # clean all things up in these cases
          return 0;;
       purge,*)    # clean all things up in these cases
          return 0;;
       *) return 1;; # some sort of overlapping upgrade is proceeding
    esac
}

can_erase_config() {
    chk="${PRE_INST_ARGS[0]}${POST_INST_ARGS[0]},${PRE_INST_ARGS[1]}${POST_INST_ARGS[1]}"
    case $chk in
       purge,)   # certainly can remove all config
          return 0;;
       *)          # config must be left...
          return 1;;
    esac
}

PKG_SCRIPT_ARGS=("${PRE_INST_ARGS[@]}" "${POST_INST_ARGS[@]}" "${PRE_RM_ARGS[@]}" "${POST_RM_ARGS[@]}")


#
# post-RM scripts run at the correct time (before post-trans) 
#

if [ $pkgscript = RPM -a "${PRE_RM_ARGS[0]}" = 1 ]; then
    echo "========================== (avoid repeat) [$RPM_PACKAGE_NAME-$RPM_PACKAGE_VERSION] args: ${PKG_SCRIPT_ARGS[@]} ${!PRE_*}${!POST_*} ============="
    exit 0   # *should* be already handled earlier... so dont run again!
fi

echo "========================== [$RPM_PACKAGE_NAME-$RPM_PACKAGE_VERSION] args: ${PKG_SCRIPT_ARGS[@]} ${!PRE_*}${!POST_*} ============="

set -${setopt/s}
