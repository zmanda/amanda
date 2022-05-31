#!/usr/bin/env bash
# Common Functions

# Required variables:
# LOGFILE
# SYSCONFDIR
# os

setopt=$-
set +xv
set +o posix

AMANDAHOMEDIR=${AMANDAHOMEDIR:-/var/lib/amanda}

logger() {
	# A non-annoying way to log stuff
	# ${@} is all the parameters, also known as the message.  Quoting the input
	# preserves whitespace.
	msg="`date +'%b %d %Y %T'`: ${@}"
        [ -z "$LOGFILE" ] || LOGFILE=/var/log/amanda/install.log
        touch 2>/dev/null "${LOGFILE}" && echo "${msg}" >> ${LOGFILE}
        touch 2>/dev/null "${LOGFILE}" || echo "${msg}"
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

check_superserver() {
    local type=$1
    local svc=$2

    # Check for the superserver $1 for the config $2
    case $type in
	xinetd) check_xinetd $svc;;
	inetd) check_inetd $svc;;
	launchd) check_launchd $svc;;
        smf) check_smf $svc;;
        *) false ;;
    esac
}

check_xinetd() {
    # Checks for an xinetd install and a config name passed as the first
    # parameter.

    #	 2 if xinetd.d/ does not exist or is a file
    [ -d ${SYSCONFDIR}/xinetd.d ]      || return 2

    #	 1 if it does not,
    [ -f ${SYSCONFDIR}/xinetd.d/${1} ] || return 1

    #	 0 if the file exists,
    logger "Found existing xinetd config: ${1}"
    return 0
}

check_inetd() {
    case $os in
        SunOS) inetd_conf=${SYSCONFDIR}/inet/inetd.conf ;;
        *) inetd_conf=${SYSCONFDIR}/inetd.conf ;;
    esac
    [ -e ${inetd_conf} ]         || return 2

    grep -q "${1}" ${inetd_conf} || return 1

    logger "Found existing inetd config: ${1}"
    return 0
}

check_launchd() {
    # TODO: refactor OS X scripts.
    :
}

check_smf() {
    local svc="${1:-amandaclient}"
    # Only for solaris! Check if the given service is active
    # it does not notice server vs client entries.
    log_output_of svcs -H "*${svc}*" || { \
        logger "No $svc service found."; return 1; }
}

check_superserver_running() {
    # Check for the given superserver, $1, in the output of ps -ef, or on
    # mac/bsd ps ax.
    # Return codes:
    #  0: $1 is running
    #  1: $1 is not running
    #  2: $1 is not valid for this system
    case $1 in
	# Linux or Solaris.  This works despite sol10 using SMF.
	inetd) ps_flags='-e';;
	xinetd) ps_flags='-e';;
	# Mac OS X
	launchd) ps_flags='aux';;
	*) echo "Bad superserver."; return 2 ;;
    esac
    if [ "$1" = "launchd" ] && [ `uname` != 'Darwin' ]; then
	echo "Only darwin uses launchd"
	return 2
    fi
    if [ "$1" = "xinetd" ] && [ "$os" = 'SunOS' ] && \
       [ `uname -r` = "5.10" ]; then
        echo "Solaris 10 does not use xinetd."
        return 2
    fi
    # Search for $1,
    PROC=`ps ${ps_flags} | grep -v 'grep'| grep "${1}"`
    if [ x"${PROC}" != x ]; then
	return 0
    else
	return 1
    fi
}

backup_xinetd() {
    log_output_of mv ${SYSCONFDIR}/xinetd.d/${1} ${AMANDAHOMEDIR}/example/xinetd.${1}.orig || \
	{ logger "WARNING:  Could not back up existing xinetd configuration '${1}'";
	return 1; }
    logger "Old xinetd config for '${1}' backed up to '${AMANDAHOMEDIR}/example/xinetd.${1}.orig'"
}

backup_inetd() {
    local inetd_conf=${SYSCONFDIR}/inet/inetd.conf 
    # Backs up any amanda configuration it finds
    log_output_of sed -n "/^amanda .* amandad/w ${AMANDAHOMEDIR}/example/inetd.orig" ${inetd_conf} || \
	{ logger "WARNING: could not write ${AMANDAHOMEDIR}/example/inetd.orig"; return 1; }
    log_output_of sed "/^amanda .* amandad/d" ${inetd_conf} > \
	${inetd_conf}.tmp || \
	{ logger "WARNING: could not write ${inetd_conf}.tmp"; return 1; }
    log_output_of mv ${inetd_conf}.tmp ${inetd_conf} || \
	{ logger "WARNING: could not overwrite ${inetd_conf}, old config not removed."; return 1; }
    logger "Old inetd config for amanda backed up to ${AMANDAHOMEDIR}/example/inetd.orig"
}

rm_smf() {
    local svc=$1
    # Remove the amanda service from solaris style service framework.
    # Different releases have different proceedures:
    #   Solaris 10, OpenSolaris 11, Nexenta, OpenIndiana, Illumos:
    #     use svccfg + delete xml
    #   Oracle Solaris 11: delete xml file + svcadm restart manifest-import,
    #     but openSolaris process still works.
    # svccfg delete is not recommended for Oracle Solaris 11, but doesn't seem
    # to hurt.  It's the only method for open source variants (so far).

    local r

    logger "Removing $svc's smf entry"
    log_output_of svcadm disable svc:/network/$svc/tcp || r=1
    log_output_of svccfg delete svc:/network/$svc/tcp || r=1
    log_output_of rm /var/svc/manifest/network/$svc.xml || r=1
    return $r
}

backup_smf() {
    local svc=$1

    # We assume the service has already been checked with check_smf!
    log_output_of svcs -H "*${svc}*" ||
        { logger "No $1 service found."; return 1; }

    log_output_of svccfg export "$svc/tcp" > ${AMANDAHOMEDIR}/example/$svc.xml.orig ||
        { logger "Warning: export of existing $svc service failed."; return 1; }

    rm_smf $svc ||
       logger "Warning: existing smf instance may not have been removed."
}

install_xinetd() {
    log_output_of install -m 0644 ${AMANDAHOMEDIR}/example/xinetd.${1} ${SYSCONFDIR}/xinetd.d/${1} || \
       { logger "WARNING:  Could not install xinetd configuration '${1}'" ; return 1; }
    logger "Installed xinetd config for ${1}."
}

install_inetd() {
    local inetd_conf=${SYSCONFDIR}/inet/inetd.conf 
    # This one is hard to log because we're just appending.
    logger "Appending ${AMANDAHOMEDIR}/example/inetd.conf.${1} to ${inetd_conf}"
    cat ${AMANDAHOMEDIR}/example/inetd.conf.${1} >> ${inetd_conf}
}

install_smf() {
    local svc=$1

    backup_smf $svc && { logger "$svc service found.  Backed up and removed."; }

    # First parameter should be the name of the service to install
    # (amandaserver, amandaclient, or zmrecover).
    case $(uname -r) in
      5.1?)
        [ $(uname -r) = 5.11 ] && logger "Solaris 11 should use IPS packages, not post-install scripts."

        # Some versions this imports, enables, and onlines a service. Others only import.
        # To be safe, we run svcadm at the end, which won't hurt.
        # log_output_of cp ${AMANDAHOMEDIR}/example/$svc.xml /var/svc/manifest/network/

        log_output_of ${basedir}/usr/sbin/svccfg import ${AMANDAHOMEDIR}/example/$svc.xml || {
            logger "Warning: Failed to import $svc SMF manifest. Check the system log.";
            return 1;
        }

        logger "Installed $svc manifest."

        log_output_of ${basedir}/usr/sbin/svcadm enable network/$svc/tcp || {
            logger "Warning: Failed to enable $svc service.  See system log for details."
            return 1;
        }

        logger "Enabled $svc service."
        [ $(uname -r) = 5.11 ] && log_output_of svcadm restart manifest-import
      ;;

      5.8|5.9)
        logger "Solaris 8 and 9 use inetd, not SMF tools."
        return 1
      ;;

      *)
        # I don't know what to do...
        logger "ERROR: Unsupported and untested version of Solaris: $ver"
        return 1
      ;;
    esac
}

reload_xinetd() {
    # Default action is to try reload.
    case $1 in
        reload|restart|start) action=$1 ;;
        "") action=reload ;;
        *) logger "WARNING: bad argument to reload_xinetd: $1"
           return 1
        ;;
    esac
    RCDIR=/dev/null
    [ -d ${SYSCONFDIR}/init.d ] && RCDIR=${SYSCONFDIR}/init.d || true
    [ -d ${SYSCONFDIR}/rc.d/init.d ] && RCDIR=${SYSCONFDIR}/rc.d/init.d || true

    if [ -f ${RCDIR}/xinetd ]; then
        if [ "$action" = "reload" ] ; then
            logger "Reloading xinetd configuration..."
	    log_output_of ${RCDIR}/xinetd $action # Don't exit!
                if [ $? -ne 0 ] ; then
                    logger "xinetd reload failed.  Attempting restart..."
                        action=restart
                fi
        fi
        if [ "$action" = "restart" ] || [ "$action" = "start" ]; then
            logger "${action}ing xinetd."
	    log_output_of ${RCDIR}/xinetd $action || \
	        { logger "WARNING:  $action failed." ; return 1; }
        fi
    elif systemctl status xinetd.service | grep -q .; then
        if [ "$action" = "reload" ] ; then
	    logger "Reloading xinetd configuration..."
            if ! log_output_of systemctl $action xinetd.service; then
                    logger "xinetd reload failed.  Attempting restart..."
                        action=restart
                fi
            fi
        if [ "$action" = "restart" ] || [ "$action" = "start" ]; then
            logger "${action}ing xinetd."
	    log_output_of systemctl $action xinetd.service || \
                { logger "WARNING:  $action failed." ; return 1; }
            fi
    elif [ ! -x /usr/sbin/xinetd ]; then
       logger "WARNING:  $action xinetd not found." ;
       return 1;
    elif [ / -ef /proc/1/root/. ]; then
       logger "WARNING:  $action not in chroot and systemctl failed." ;
       return 1;
    fi
    # if in chroot.. go and start manually...
    ( /usr/sbin/xinetd & )
}

get_substitute_output()
{
    { echo -n "$*" | perl $(dirname "${BASH_SOURCE[0]}")/substitute.pl /dev/stdin /dev/stdout; }  || 
       exit 255
}

get_full_suffix()   { get_substitute_output '%%VERSION%%-%%PKG_SUFFIX%%'; }
get_pkg_suffix()    { get_substitute_output '%%PKG_SUFFIX%%'; }
get_glib2_version() { get_substitute_output '%%BUILD_GLIB2_VERSION%%'; }


create_root_cert() {
   local private
   local public
   local ext509opts

   private=$1
   public=$2

   touch $1 $2 || return 1;
   (
     p=$(command -v openssl)
     p=${p%/bin/openssl}
     export OPENSSL_CONF=${p%/usr}/etc/pki/tls/openssl.cnf;
     rm -f ${public%.*}.idx
     touch ${public%.*}.idx
     ext509opts=''
     ext509opts+=$'keyUsage=critical, keyCertSign, cRLSign\n'
     ext509opts+=$'basicConstraints=critical,CA:true,pathlen:1\n'
     ext509opts+=$'subjectKeyIdentifier=hash\n'

     config_opts+=$'.include /etc/pki/tls/openssl.cnf\n\n'
     config_opts+=$'[ CA_default ]\n'
     config_opts+=$'dir             = .\n'
     config_opts+="database   =  ${public%.*}.idx"$'\n'
     config_opts+="serial     =  ${public%.*}.srl"$'\n'
#     config_opts+=$'certs           =\n'
     config_opts+=$'new_certs_dir   = .\n'

     [ -s $private ] || openssl genpkey -algorithm rsa -pkeyopt rsa_keygen_bits:2048 -out $private
     openssl req -new -sha256 -key $private -multivalue-rdn \
            -subj "/CN=BETSOL/C=US/ST=Colorado/L=Default City/O=Betsol Inc/OU=Zmanda" |
     openssl ca -in /dev/stdin \
           -days 3650  \
           -config <(echo "$config_opts") \
           -extfile <(echo "$ext509opts") \
           -keyfile $private \
           -selfsign \
           -batch -rand_serial \
           -notext \
           -out $public

     #openssl x509 -req -sha256 -days 3650 -in /dev/stdin -extfile <(echo "$ext509opts")
     # -signkey $private -trustout -out $public
   )
   chmod 600 $private
   chmod 644 $public
   [ -s $private -a -s $public ]
}


create_self_signed_key() {
   local private
   local public
   local fqdn=
   local ipaddr=
   local hostname=$(uname -n)
   local altnames="DNS:zmanda-zmc"

   p=$( PATH=${PATH}:/sbin:/usr/sbin command -v ip)
   ipaddr=$($p -o route get 1.1.1.1 | sed -e 's/^.* src[^0-9.]*//' -e 's/[^0-9.].*//')
   private=$1
   public=$2
   fqdn=$(echo -e 'import socket\nprint(socket.getfqdn())' | ${PYTHON:-python})
   [ -z "$fqdn" ] && fqdn=$(perl -e 'use Net::Domain qw(hostfqdn); print hostfqdn();')
   altnames+=",DNS:$fqdn"
   altnames+=",DNS:$ipaddr"
   altnames+=",IP:$ipaddr"
   [ "$hostname" != "$fqdn" ] && [[ "$hostname" != *[^a-zA-Z0-9.-]* ]] &&
	   altnames+=",DNS:${hostname%.local}.local"
   rm -f $public $private || return 1;
   touch $public $private || return 1;
   (
     p=$(command -v openssl)
     p=${p%/bin/openssl}
     export OPENSSL_CONF=${p%/usr}/etc/pki/tls/openssl.cnf;
     [ -s $private ] || openssl genpkey -algorithm rsa -pkeyopt rsa_keygen_bits:2048 -out $private
     openssl req -x509 -nodes -days 365 -key $private -multivalue-rdn -out $public \
          -subj "/CN=$fqdn/emailAddress=root@$fqdn/C=US/ST=Colorado/L=Default City/O=Betsol Inc/OU=Zmanda" \
	  -addext "subjectAltName=\"$altnames\"" \
          -addext "certificatePolicies = 2.5.29.32.0"
     [ -s $private -a -s $public ] || exit 255
   )
   rmdir private-keys-v1.d || true
   chmod 600 $private
   chmod 644 $public
}


#   tee out.priv |
#   openssl req -x509 -nodes -days 365 -key /dev/stdin -multivalue-rdn -subj \
#        "/CN=$fqdn/emailAddress=root@$fqdn/C=US/ST=Colorado/L=Default City/O=Betsol Inc/OU=Zmanda" \
#        -keyout out.priv -out out.pub

   # openssl req -new -newkey rsa:2048 -keyout ca.key -out ca.pem
   # openssl ca -create_serial -out cacert.pem -days 365 -keyfile ca.key -selfsign -infiles ca.pem

server_reset_tools() {
    (
        local patt="${INSTALL_TOPDIR//\//?}"
        export PATH="${PATH//${patt}/++nopath++}"
        export PATH="/usr/bin:/usr/sbin:/bin:/sbin:${PATH}"
        find ${INSTALL_TOPDIR}/bin -lname /bin/false -printf " %f\n" |
         while read cmd; do
            path="$(command -v $cmd 2>/dev/null)"
            path="$(realpath -e $path 2>/dev/null)"
            [ ! -x "$path" ] && continue
            ln -sf "$path" ${INSTALL_TOPDIR}/bin/$cmd &&
                echo $'\tenabled: '"$cmd -> $path";
         done

        #
        # more tries to assign mailx and gtar symlinks...
        #
        Mail_path=$(command -v Mail 2>/dev/null)
        mail_path=$(command -v mail 2>/dev/null)
        tar_path=$(command -v tar 2>/dev/null)
        [ ${INSTALL_TOPDIR}/bin/mailx -ef /bin/false -a -x "$Mail_path" ] &&
           ( set -x; ln -sf $Mail_path ${INSTALL_TOPDIR}/bin/mailx )
        [ ${INSTALL_TOPDIR}/bin/mailx -ef /bin/false -a -x "$mail_path" ] &&
           ( set -x; ln -sf $mail_path ${INSTALL_TOPDIR}/bin/mailx )
        [ ${INSTALL_TOPDIR}/bin/gtar -ef /bin/false -a -x "$tar_path" ]  &&
           ( set -x; ln -sf $tar_path ${INSTALL_TOPDIR}/bin/gtar )

        # unlikely to need it ... but this is where solaris keeps its version...
        [ -x /usr/sbin/tar ] &&
           ( set -x; ln -sf /usr/sbin/tar ${INSTALL_TOPDIR}/bin/suntar )
   )
}

server_init_tools() {
    local alt_cmds=(
       mailx xfsdump xfsrestore dump
       restore smbclient bsdtar tar gtar star
       lpr gnuplot mt mtx
    )

# creates symlink in CWD (by default with 1 arg) or nothing with zero..
    for cmd in ${alt_cmds[@]}; do
        ln -sf /bin/false "${INSTALL_TOPDIR}/bin/$cmd"
    done
    server_reset_tools
}

shutdown_xinetd_socket() {
    local sockunit=$1
    local ports
    local greps

    systemctl is-enabled $sockunit | grep -q enabled ||
       return 1

    rm -f /etc/xinetd.d/amandaserver /etc/xinetd.d/zmrecover /etc/xinetd.d/amandaclient
    rm -f /etc/inetd.d/amandaserver /etc/inetd.d/zmrecover /etc/inetd.d/amandaclient
    ( set -x; systemctl reload xinetd 2>/dev/null ||
              systemctl restart xinetd 2>/dev/null ||
              true )  # if it exists?

    readarray -t ports < <(systemctl --no-legend list-sockets $sockunit)
    ports=("${ports[@]%%/*}");  # remove files entirely
    ports=("${ports[@]%% *}")
    ports=("${ports[@]##*[^0-9]}")
    ports=("${ports[@]%%[^0-9]*}")
    ports=(${ports[*]})

    # must stop if no ports found
    [ ${#ports[@]} -gt 0 ] ||
       return 1

    greps=()
    for i in ${!ports[*]}; do
        greps+=('-e')
        greps+=("$(printf ": 0\\+:%04x" ${ports[$i]})")
    done

    for i in {1..20}; do
        echo "waiting ${i} --> 20 for socket listening to port $port to close..."
        ( set -x; grep -qr "${greps[@]}" /proc/net/tcp*; ) || break
        sleep 1
    done
}

#
# place replacement paths right after standard parts of the *generated* amanda-security.conf file!
# (if lines were edited out: do NOT add in.. and don't add at end of file)

fix_security_conf_path() {
    local security_path=$1
    local cmd=$2
    local label=$3
    local path="$(command -v $cmd 2>/dev/null)"

    # missing --> do nothing..
    [ -x "$path" ]             || 
       return

    # same as /bin/false --> do nothing..
    path="$(readlink -e $path)"
    [ $path -ef /bin/false ] && 
       return

    # already in the file --> do nothing..
    grep -q "^${label}_path=$path\$" $security_path && 
       return

    ( set -x; sed -i -e "\|^#${label}_path=|s|\$|\n${label}_path=$path|" $security_path)
}

fix_security_conf() {
    local pathadd=$1
    local security_path="${SYSCONFDIR}/amanda-security.conf"

    (
        export PATH=${pathadd}:${PATH};
        if [ $(uname) = SunOS ]; then
            fix_security_conf_path "$security_path" tar   amstar:star
        else
           fix_security_conf_path "$security_path" tar    runtar:gnutar
           fix_security_conf_path "$security_path" tar    amgtar:gnutar
        fi
        fix_security_conf_path "$security_path" gtar   runtar:gnutar
        fix_security_conf_path "$security_path" gtar   amgtar:gnutar
        fix_security_conf_path "$security_path" bsdtar ambsdtar:bsdtar
    )

    #
    # try to comment any allowed paths that dont exist!
    #
    approved=($(sed -e '/^[^#=]\+=/!d' -e 's/^[^=]*=//' $security_path))
    for path in ${approved[@]}; do
        path=$(echo ${path})
        [[ $path == /* ]] || continue     # ignore non-absolute-path strings...
        [ -e "$path" ] && continue        # leave existing paths alone...
        # something is wrong...
        ( set -x; sed -i -e "\,${path}\$,s,^,### INVALID-PATH ###," $security_path)
    done
}

create_dynamic_keys() {
    #
    # create dynamic keys
    #
    # remove current gpg key if check fails...
    check_gnupg 2>/dev/null ||
       rm -f ${AMANDAHOMEDIR}/.gnupg/am_key.gpg

    # get a random passphrase from 60 bytes of binary into full ASCII85
    [ -s ${AMANDAHOMEDIR}/.am_passphrase ] ||
        get_random_ascii_lines_perl 1  |
        tee -a ${AMANDAHOMEDIR}/.am_passphrase >/dev/null

    # create am_key.gpg from the new/old .am_passphrase if needed
    [ -s ${AMANDAHOMEDIR}/.gnupg/am_key.gpg ] ||
        create_amkey || \
        logger "Info: amcrypt will not work until keys are created."

    # check again if pair is associated and ready.. but fail if needed
    check_gnupg

    [ -s ${AMANDAHOMEDIR}/.ssh/id_rsa_amdump ] ||
        {
            rm -f ${AMANDAHOMEDIR}/.ssh/id_rsa_amdump*;  # erase any zero-length files
            log_output_of ssh-keygen -q -C ${amanda_user}@server -t rsa \
                  -f ${AMANDAHOMEDIR}/.ssh/id_rsa_amdump -N '' </dev/null
        } ||
        logger "Info: failed to create server amdump ssh key"

    [ -s ${AMANDAHOMEDIR}/.ssh/id_rsa_amrecover ] ||
        {
            rm -f ${AMANDAHOMEDIR}/.ssh/id_rsa_amrecover*;  # erase any zero-length files
            log_output_of ssh-keygen -q -C ${amanda_user}@client -t rsa \
                  -f ${AMANDAHOMEDIR}/.ssh/id_rsa_amrecover -N '' </dev/null
        } ||
        logger "Info: failed to create client amrecover ssh key"
}


#####################################################################
# shared config restore function
restore_saved_configs() {
    local rpmname="${RPM_PACKAGE_NAME}"
    local cfgs=()
    local cfgs_saves=()
    local debname=${rpmname//_/-}

    #
    # must restore (or clean up) any old saved-config files in order of priority
    #
    # all config files in %files section...
    cfgs=()

    if command -v rpm >/dev/null && rpm -q "$rpmname"  >&/dev/null; then
        readarray -t cfgs < <(rpm -qc "$rpmname")
    elif command -v dpkg-query >/dev/null && dpkg-query -W "$debname" >&/dev/null; then
        readarray -t cfgs < <(dpkg-query --control-show "$debname" conffiles)
    elif [ $# -gt 1 ]; then
        :
    else
        # echo >&2 "ERROR: failed to query config files"
        return 1
    fi

    cfgs+=("$@")

    # leave this restore behind if no configs exist anyway...
    if [ "${#cfgs[@]}" = 0 ]; then
        return 0
    fi

    # subst all / with \/
    homedir_patt="${AMANDAHOMEDIR//\//[/]}"

    # add searches to old home dir
    if [ -d ${AMANDAHOMEDIR}-1/. ]; then
        home_cfgs=( $(IFS=$'\n'; grep <<<"${cfgs[*]}" "^${AMANDAHOMEDIR}/") )
        cfgs+=( "${home_cfgs[@]/#${homedir_patt}/${AMANDAHOMEDIR}-1}" )
    fi

    # on a new install ... default is installed and edited config files are .rpmsave
    # otherwise existing config files are preserved (and a .rpmnew is created)
    # [ordered by priority of best result]

    # dirs are valid config ---if they are not empty---
    # preserve non-default in-place cfgs as lowest priority updates
    for i in $(ls -d 2>/dev/null ${cfgs[@]}); do
        [ -e "$i" -o -L "$i" ] || continue
        { find "$i" -maxdepth 0 -empty | grep -q .; } && continue
        mv $i $i.__existing__        # rename to keep cleaned up
        cfgs_saves+=($i.__existing__)
    done

    cfgs_saves+=( $(ls -d 2>/dev/null ${cfgs[@]/%/.cfgsave}) )
    cfgs_saves+=( $(ls -d 2>/dev/null ${cfgs[@]/%/.rpmsave}) )
    cfgs_saves+=( $(ls -d 2>/dev/null ${cfgs[@]/%/.dpkg-old}) )
    cfgs_saves+=( $(ls -d 2>/dev/null ${cfgs[@]/%/.dpkg-dist}) )
    cfgs_saves+=( $(ls -d 2>/dev/null ${cfgs[@]/%/.rpmnew}) )
    cfgs_saves+=( $(ls -d 2>/dev/null ${cfgs[@]/%/.dpkg-new}) )

    for fnd in ${cfgs_saves[@]}; do
        { find "$fnd" -maxdepth 0 -empty | grep -q .; } && {
            rm -f "$fnd" || rmdir "$fnd" 2>/dev/null
            continue;  # ignore ALL zero-length files or dirs
        }

        #
        # wherever it was found, get the target dest
        #
        tgt="$fnd"
        tgt="${tgt/#${homedir_patt}-1/${AMANDAHOMEDIR}}"
        tgt="${tgt%.cfgsave}"
        tgt="${tgt%.rpmsave}"
        tgt="${tgt%.dpkg-old}"
        tgt="${tgt%.dpkg-dist}"
        tgt="${tgt%.rpmnew}"
        tgt="${tgt%.dpkg-new}"
        tgt="${tgt%.__existing__}"

        [ "$tgt" = "$fnd" ] &&
           echo >&2 "WARNING: old config file left alone: $fnd "
#        [ "$tgt" != "$fnd" ] && [[ "$fnd" != *.__existing__ ]] &&
#           echo >&2 "NOTE: earlier config file re-used: $tgt "
#        [ "$tgt" != "$fnd" ] && [[ "$fnd" == *.__existing__ ]] &&
#           echo >&2 "NOTE: installed config file retained: $tgt "

        [ -d "$tgt" ] && rmdir "$tgt" 2>/dev/null 
        [ "$tgt" != "$fnd" ] && mv -fv $fnd $tgt
    done
    set +xv
}


# End Common functions
set -${setopt/s}
