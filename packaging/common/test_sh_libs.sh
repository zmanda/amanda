#!/bin/sh
# This script is uses ShUnit to test packaging/common/*.  Currently
# it only tests the functions as they perform for rpm and deb building.
# As support is added for other styles of package, these tests should be
# updated.

#######################
# If EXECUTING WITH ROOT ACCESS SCRIPT DELETES AND CREATES USERS.
# EXECUTE WITH CAUTION.
#######################

#set -x

# Note on conflicting variable names:
# SHUNIT_* and SHU_* are shunit2 variables
# test_* should only be used by test functions.
# All other variables will probably be used by pre/post/common functions.

# shunit2 assumes _inc is, but we might run from a separate builddir. So try to
# use srcdir, as defined by automake, or otherwise `pwd`.
SHUNIT_INC="${srcdir=`pwd`}/packaging/common"; export SHUNIT_INC

TMPDIR=`pwd`/shunit-test; export TMPDIR
amanda_user=test_amandabackup; export amanda_user
amanda_group=test_disk; export amanda_group
sup_group="test_tape"; export sup_group
AMANDAHOMEDIR=$TMPDIR/amanda; export AMANDAHOMEDIR
AMANDATES=$AMANDAHOMEDIR/amandates; export AMANDATES
os=`uname`; export os
wanted_shell='/bin/false'; export wanted_shell
dist=Fedora; export dist
SYSCONFDIR=$TMPDIR/etc; export SYSCONFDIR
SBINDIR=$TMPDIR/sbin; export SBINDIR
# Don't potentially conflict with a real value...
deb_uid=63000; export deb_uid


# Append stuff to this when you make temp files if they are outside TMPDIR
test_cleanup_files="$TMPDIR"; export test_cleanup_files

# This can't be part of one-time setup, because TMPDIR must exist before
# shunit2 is sourced.
mkdir -p ${TMPDIR} || exit 1
{
    LOGFILE=`
	(umask 077 && mktemp "$TMPDIR/test-log.XXXX") 2> /dev/null
	` &&
	test -f "$LOGFILE"
} || {
    LOGFILE=$TMPDIR/test-log.$$.$RANDOM
    (umask 077 && touch "$LOGFILE")
} || {
	echo "Unable to create log file!"
	exit 1
}
export LOGFILE

oneTimeSetUp() {
    msg_prefix="oneTimeSetUp: "
    if [ "$IAmRoot" ]; then
	# Delete an existing user.
        echo "$msg_prefix attempting to delete an existing user."
	userdel -f -r ${amanda_user}
        # If the user exists, our tests will act wrong, so exit
        grep "${amanda_user}" ${SYSCONFDIR}/passwd &> /dev/null || { \
            echo "$msg_prefix ${amanda_user} still exists, or no ${SYSCONFDIR}/passwd.";
            exit 1; }
        groupdel ${amanda_group} || exit 1
        groupdel ${sup_group} || exit 1
    else
	echo "Not root, cleanup skipped"
    fi
    # Make all directories which might be needed.
    mkdir -p ${AMANDAHOMEDIR}/example
    if [ ! "$IAmRoot" ]; then
        mkdir -p ${SYSCONFDIR}/xinetd.d
        mkdir -p ${SYSCONFDIR}/init.d
    fi
}

oneTimeTearDown() {
    if [ ${__shunit_assertsFailed} -eq 0 ]; then
	rm -rf $test_cleanup_files
    else
	echo "Check ${test_cleanup_files} for logs and error info."
    fi
}

test_cleanup_files="$LOGFILE ${test_cleanup_files}"

# shows syntax errors before the tests are run.
. ${SHUNIT_INC}/common_functions.sh
. ${SHUNIT_INC}/pre_inst_functions.sh
. ${SHUNIT_INC}/post_inst_functions.sh
. ${SHUNIT_INC}/post_rm_functions.sh

# Use this to skip root requiring tests.
# id -u is not portable to solaris.
tester_id=`id | sed 's/gid=\([0-9]*\).*/\1/'`
if [ "$tester_id" = "0" ]; then
    # Same as SHUNIT_TRUE, but we can't use that yet...
    IAmRoot=0
else
    IAmRoot=
fi

# CAUTION: using real values if we are root.
if [ "$IAmRoot" ]; then
    amanda_group=disk; export amanda_group
    SYSCONFDIR=/etc; export SYSCONFDIR
    SBINDDIR=/sbin; export SBINDDIR
fi

# Source our mock utils.
. ${SHUNIT_INC}/mock_utils.sh

######################################
# Common functions

# shUnit reorders tests alphabetically.  We need the logger and cleanup tests
# to run first.
test___logger() {
    # Write a line to the log, test that it got there.
    TEST_MSG="test___logger message"
    LOG_LINE="`date +'%b %e %Y %T'`: ${TEST_MSG}"
    # It's important for the log messages to be quoted, or funny stuff happens.
    logger "${TEST_MSG}"
    assertEquals "logger() return code" 0 $?
    LOG_TAIL=`tail -1 ${LOGFILE}`
    assertEquals "logger() did not write <${LOG_LINE}> " \
	"${LOG_LINE}" "${LOG_TAIL}"
    # Leave this outside the unit test framework.  if the logger is
    # broken we must exit.
    if [ ! `grep -c "${LOG_LINE}" ${LOGFILE}` = "1" ]; then
	echo "error: logger(): Incorrect content in ${LOGFILE}: " `cat ${LOGFILE}`
	exit 1
    fi
}

test__log_output_of() {
    # Use log_output_of to append to the log
    TEST_MSG="test__log_output_of message"
    log_output_of echo "${TEST_MSG}"
    assertEquals "log_output_of()" 0 $?
    COUNT=`grep -c "${TEST_MSG}" ${LOGFILE}`
    assertEquals "log_output_of(): incorrect content in log" \
	1 ${COUNT}
    # Leave this outside the unit test framework.  if the logger is
    # broken we must exit.
    if [ ! ${COUNT} = '1' ]; then
	echo "error: log_output_of(): Incorrect content in ${LOGFILE}: " `cat ${LOGFILE}`
	exit 1
    fi
}


# NOTE: check_xinetd and check_inetd tests end up simply duplicating the
# code of the function itself

test_check_smf() {
    logger "test_check_smf"
    [ "$os" = "SunOS" ] || { startSkipping; echo "test_check_smf: skipped"; }
    # Test existing service
    touch ${MOCKDIR}/prexisting_service
    check_smf
    assertEquals "check_smf preexisting services" 0 $?
    rm ${MOCKDIR}/prexisting_service
    check_smf
    assertEquals "check_smf no amanda service" 1 $?
}

test_check_superserver_running() {
    logger "test_check_superserver_running"
    [ "$IAmRoot" ] && { startSkipping; echo "test_check_superserver_running: skipped"; }
    # Test the positive cases
    # running changes the output of mocked ps
    touch ${MOCKDIR}/running
    check_superserver_running inetd
    assertEquals "check_superserver_running inetd" 0 $?
    assertSame "ps args: -e" "`cat $mock_ps_flags`"
    if [ `uname` = 'Darwin' ]; then
	[ "$IAmRoot" ] && startSkipping
        check_superserver_running launchd
        assertEquals "check_superserver_running launchd" 0 $?
        assertSame "ps args: aux" "`cat $mock_ps_flags`"
	endSkipping
    else
	endSkipping
        check_superserver_running launchd
        assertEquals 2 $?
    fi
    # Test the negative case, skip if root
    [ "$IAmRoot" ] && startSkipping
    rm ${MOCKDIR}/running
    check_superserver_running inetd
    assertEquals "check_superserver_running inetd returned 0 when inetd was not running" 1 $?
    assertSame "ps args: -e" "`cat $mock_ps_flags`"
    check_superserver_running xinetd
    assertNotEquals "check_superserver_running xinetd incorrectly returned 0" \
	 0 $?
    assertSame "ps args: -e" "`cat $mock_ps_flags`"
}

test_backup_xinetd() {
    logger "test_backup_xinetd"
    touch ${SYSCONFDIR}/xinetd.d/amandaserver
    backup_xinetd "amandaserver"
    assertEquals "backup_xinetd returns 0" 0 $?
    assertTrue "[ -f ${AMANDAHOMEDIR}/example/*.amandaserver.orig ]"
}

test_backup_inetd() {
    logger "test_backup_inetd"
    case $os in
        SunOS) inetd_dir=${SYSCONFDIR}/inet ;;
        *) inetd_dir=${SYSCONFDIR} ;;
    esac
    [ -d "${inetd_dir}" ] || mkdir -p ${inetd_dir}
    touch ${inetd_dir}/inetd.conf
    echo "amanda foo/bar/baz  amandad" >> ${inetd_dir}/inetd.conf
    backup_inetd
    assertEquals "backup_inetd returns 0" 0 $?
    assertTrue "[ -f ${AMANDAHOMEDIR}/example/inetd.orig ]"
}

test_backup_smf() {
    logger "test_backup_smf"
    :
    # TODO: how to mock this?
}

test_install_xinetd() {
    logger "test_install_xinetd"
    if [ "$os" = "SunOS" ] ; then
        # Solaris has install_xinetd_sol
        startSkipping
        echo "test_install_xinetd: skipped"
        return
    fi
    # Test success:
    touch ${MOCKDIR}/success
    touch ${AMANDAHOMEDIR}/example/xinetd.amandaserver
    install_xinetd "amandaserver"
    assertEquals "install_xinetd returns 0" 0 $?
    # Test "install" failure
    rm ${MOCKDIR}/success
    install_xinetd "amandaserver"
    assertEquals "install_xinetd returns 1" 1 $?
}

test_install_inetd() {
    logger "test_install_inetd"
    case $os in
        SunOS) inetd_dir=${BASEDIR}/${SYSCONFDIR}/inet ;;
        *) inetd_dir=${SYSCONFDIR} ;;
    esac
    [  -f ${inetd_dir}/inetd.conf ] || touch ${inetd_dir}/inetd.conf
    test_inetd_entry='amanda foo/bar/baz  amandad'
    if [ ! -f ${AMANDAHOMEDIR}/example/inetd.conf ]; then
	echo "${test_inetd_entry}" > ${AMANDAHOMEDIR}/example/inetd.conf.amandaserver
    fi
    install_inetd amandaserver
    assertEquals "install_inetd returns 0" 0 $?
    assertSame "${test_inetd_entry}" "`tail -1 ${inetd_dir}/inetd.conf`"
}

# TODO: test_install_smf() {
# Needs mocks for: inetconv, inetadm, svcadm.

test_reload_xinetd() {
    logger "test_reload_xinetd"
    # Might need init script.
    if [ "$IAmRoot" ]; then
        startSkipping
        echo "test_install_smf: skipped"
        return
    elif [ ! -f "${SYSCONFDIR}/init.d/xinetd" ]; then
        mv ${MOCKDIR}/xinetd ${SYSCONFDIR}/init.d
    fi
    # Test bad argument
    reload_xinetd foo
    assertEquals "reload_xinetd should reject argument 'foo'" 1 $?
    # Test success
    touch ${MOCKDIR}/success
    reload_xinetd "reload"
    assertEquals "reload_xinetd" 0 $?
    # Test failure
    rm ${MOCKDIR}/success
    reload_xinetd "reload"
    assertEquals "reload_xinetd" 1 $?
    tail -4 ${LOGFILE}|grep "\<xinetd.*Attempting restart" >/dev/null
    assertEquals "reload_xinetd should try to restart." 0 $?
    reload_xinetd "restart"
    assertEquals "restart should fail." 1 $?
    tail -3 ${LOGFILE}|grep "Restarting xinetd" >/dev/null
    assertEquals "Should log attempt to restart" 0 $?
}

test_reload_inetd() {
    logger "test_reload_inetd"
    # Test bad argument
    # Might need init script.
    if [ ! "$IAmRoot" ]; then
        if [ ! -f "${SYSCONFDIR}/init.d/inetd" ]; then
            mv ${MOCKDIR}/inetd ${SYSCONFDIR}/init.d
        fi
    fi
    # Test bad argument
    reload_inetd foo
    assertEquals "reload_inetd should reject argument 'foo' (return 1):" 1 $?
    # Test success
    touch ${MOCKDIR}/success
    reload_inetd
    assertEquals "reload_inetd" 0 $?
    # Test failure
    rm ${MOCKDIR}/success
    reload_inetd "reload"
    assertEquals "reload_inetd" 1 $?
    tail -4 ${LOGFILE}|grep "\<inetd.*Attempting restart" >/dev/null
    assertEquals "reload_inetd should try to restart." 0 $?
    reload_inetd "restart"
    assertEquals "restart should fail." 1 $?
    tail -3 ${LOGFILE}|grep "Restarting inetd" >/dev/null
    assertEquals "Should log attempt to restart" 0 $?
}


######################################
# pre_install_functions

test_check_user_group_missing() {
    logger "test_check_user_group_missing no param"
    check_user_group
    assertNotEquals "'check_user_group' should fail" 0 $?
    logger "test_check_user_group_missing missing group"
    [ ! "$IAmRoot" ] && rm -f ${SYSCONFDIR}/group
    touch ${SYSCONFDIR}/group
    for os in linux osx solaris; do
        echo $os > ${MOCKDIR}/id_os
        check_user_group "abracadabra"
        assertNotEquals "'check_user group abracadabra' should not be found:" 0 $?
        LOG_TAIL=`tail -1 ${LOGFILE}|cut -d " " -f 5-`
        assertEquals "check_user_group should write" \
            "User's primary group 'abracadabra' does not exist" \
            "${LOG_TAIL}"
    done
}

good_group_entry="${amanda_group}:x:100:"
export good_group_entry
test_check_user_group_exists() {
    logger "test_check_user_group user and group exist"
    touch ${MOCKDIR}/id_exists
    touch ${SYSCONFDIR}/group
    # Non-root adds and entry to the mock group file
    [ ! "$IAmRoot" ] && echo $good_group_entry > ${SYSCONFDIR}/group
    for os in linux osx solaris; do
        echo $os > ${MOCKDIR}/id_os

        # Case 1: Amanda_user is correct.
        echo ${amanda_group} > ${MOCKDIR}/id_group
        check_user_group "${amanda_group}"
        assertEquals "'check_user_group ${amanda_group}': id returns correct groupname" \
            0 $?

        # Case 2: Amanda_user is not a member of the the correct primary group.
        rm ${MOCKDIR}/id_group
        check_user_group "${amanda_group}"
        assertEquals "'check_user_group ${amanda_group}' when not a member" 1 $?
    done
}

test_check_user_supplemental_group_missing() {
    logger "test_check_user_supplemental_group missing"
    [ ! "$IAmRoot" ] && echo $good_group_entry > ${SYSCONFDIR}/group
    for os in linux osx solaris; do
        echo $os > ${MOCKDIR}/id_os
        check_user_supplemental_group ${sup_group}
        assertEquals "'check_user supplemental-group ${sup_group}' when group missing" \
        1 $?
    done
}

missing_group_member="${sup_group}:x:105:nobody"
export missing_group_member
good_sup_group_entry="${missing_group_member},${amanda_user}"
export good_sup_group_entry

test_check_user_supplemental_group_exists() {
    logger "test_check_user_supplemental_group exists"
    [ ! "$IAmRoot" ] && echo $missing_group_member > ${SYSCONFDIR}/group
    check_user_supplemental_group ${sup_group}
    assertEquals "'check_user_supplemental_group ${sup_group}' when amanda_user is not a member" \
        1 $?

    [ ! "$IAmRoot" ] && echo ${good_sup_group_entry} > ${SYSCONFDIR}/group
    check_user_supplemental_group ${sup_group}
    assertEquals "'check_user_supplemental_group ${sup_group}' with correct membership" \
        0 $?
}
test_check_user_shell() {
    logger "test_check_user_shell"
    if [ ! "$IAmRoot" ]; then
	echo "${good_passwd_entry}" > ${SYSCONFDIR}/passwd
    fi
    # Case 1: Provide a matching shell
    check_user_shell "/bin/bash"
    assertEquals "check_user_shell /bin/bash (matching)" 0 $?
    # Case 2: Provide a non-matching shell. 
    check_user_shell "/bin/ksh"
    assertEquals "check_user_shell /bin/ksh (not matching)" 1 $?
}

test_check_user_homedir() {
    logger 'test_check_user_homedir'
    if [ ! "$IAmRoot" ]; then
	echo "${good_passwd_entry}" > ${SYSCONFDIR}/passwd
    fi
    # Case 1: Assume amanda_user is correct.
    check_user_homedir "${AMANDAHOMEDIR}"
    assertEquals "check_user_homedir ${AMANDAHOMEDIR}" 0 $?
    # Case 2: Provide an incorrect homedir
    check_user_homedir "/tmp"
    assertEquals "check_user_homedir /tmp" 1 $?
}

test_check_user_uid() {
    echo "${amanda_group}" > ${MOCKDIR}/id_group
    touch ${MOCKDIR}/id_exists
    logger 'test_check_user_uid'
    for os in linux osx solaris; do
        echo $os > ${MOCKDIR}/id_os
        check_user_uid
        assertEquals "check_user_uid without a uid" 1 $?
        logger 'test_check_user_uid wrong id'
        check_user_uid 123
        assertEquals "check_user_uid uids don't match" 1 $?
        logger 'test_check_user_uid correct id'
        check_user_uid ${deb_uid}
    done

}
test_check_homedir_dir_missing() {
    logger "test_check_homedir_dir_missing"
    # First make sure the dir is missing
    rm -rf ${AMANDAHOMEDIR}
    check_homedir
    assertNotEquals "check_homedir returned 0, but homedir did not exist" 0 $?
}

# passwd file entry for Linux systems, maybe others.  UID is correct
# for Debian as well.
good_passwd_entry="${amanda_user}:x:${mock_deb_uid}:6::${AMANDAHOMEDIR}:/bin/bash"
export good_passwd_entry
test_create_user() {
    logger "test_create_user"
    if [ ! "$IAmRoot" ]; then
	startSkipping
	echo "test_create_user: Creating mock passwd file."
	echo "$good_passwd_entry" > ${SYSCONFDIR}/passwd
	echo "test_create_user: tests skipped."
	#TODO: mock useradd.
	return
    fi
    # Case 1: create_user should succeed.
    create_user
    assertEquals "create_user()" 0 $?
}

test_add_group_check_parameters_logs() {
    rm -f ${MOCKDIR}/groupadd_rc ${MOCKDIR}/num_groups
    # Return codes are integers.
    printf '%i' 0 > ${MOCKDIR}/groupadd_rc
    # Test that first parameter is required.
    add_group
    assertEquals "add_group without a group should fail." 1 $?
    LOG_TAIL=`tail -1 ${LOGFILE}|cut -d " " -f 5-`
    assertEquals "add_group should write" \
        "Error: first argument was not a group to add." \
        "${LOG_TAIL}"
}

test_add_group_group_ok() {
    # groupadd created group
    printf '%i' 0 > ${MOCKDIR}/groupadd_rc
    echo '${amanda_user} : prev_grp1' > ${MOCKDIR}/groups_output
    add_group twinkle
    assertEquals "add_group group ok" 0 $?
    flags=`cat ${mock_usermod_flags}`
    assertEquals "usermod_flags" \
        "usermod args: -G prev_grp1,twinkle ${amanda_user}" \
        "${flags}"

    # Make sure supplemental groups are preserved when adding groups to an
    # existing account
    echo '${amanda_user} : prev_grp1 prev_grp2' > ${MOCKDIR}/groups_output
    printf '%i' 1 > ${MOCKDIR}/num_groups
    add_group twinkle
    assertEquals "add_group group ok" 0 $?
    flags=`cat ${mock_usermod_flags}`
    assertEquals "usermod_flags should contain:" \
        "usermod args: -G prev_grp1,prev_grp2,twinkle ${amanda_user}" \
        "${flags}"
}

test_create_homedir() {
    logger "test_create_homedir"
    rm -rf ${AMANDAHOMEDIR}
    create_homedir
    assertEquals "create_homedir returns 0" 0 $?
    assertTrue "${AMANDAHOMEDIR} did not get created" "[ -d ${AMANDAHOMEDIR} ]"
    if [ "$IAmRoot" ]; then
	# Check real owner
	real_owner=`ls -ld $AMANDAHOMEDIR | awk '{ print $3":"$4; }'`
	assertSame "${amanda_user}:${amanda_group}" "$real_owner"
    else
        assertSame \
	    "chown args: -R ${amanda_user}:${amanda_group} ${AMANDAHOMEDIR}" \
	    "`cat $mock_chown_flags`"
    fi
    # A second run should succeed too.
    create_homedir    
    assertEquals "When homedir exists, create_homedir returns 0:" 0 $?
    # Later tests use ${AMANDAHOMEDIR}, so leave the dir there.
}

test_check_homedir_dir_existing() {
    logger "test_check_homedir_dir_existing"
    # Create the dir (example needed for other tests), then check again.
    mkdir -p ${AMANDAHOMEDIR}/example
    check_homedir
    assertEquals "Homedir exists; check_homedir" 0 $?
}

test_create_logdir() {
    logger "test_create_logdir"
    # The logdir variable for the shell libs, not shunit2.
    LOGDIR=$TMPDIR/amanda_log; export LOGDIR
    rm -rf ${LOGDIR}
    rm -rf ${LOGDIR}.save
    create_logdir
    assertEquals "create_logdir clean system" 0 $?
    if [ -n "$IAmRoot" ]; then
	real_owner=`ls -ld $LOGDIR | awk '{ print $3":"$4; }'`
	assertSame "${amanda_user}:${amanda_group}" "$real_owner"
    else
	assertSame \
	    "chown args: -R ${amanda_user}:${amanda_group} ${LOGDIR}"\
	    "`cat $mock_chown_flags`"
    fi
    assertTrue "${LOGDIR} exists" "[ -d ${LOGDIR} ]"
    # What happens if logdir is a file?
    rm -rf ${LOGDIR}
    touch ${LOGDIR}
    create_logdir
    assertEquals "create_logdir" 0 $?
    assertTrue "${LOGDIR} exists" "[ -d ${LOGDIR} ]"
    assertTrue "${LOGDIR}/amanda_log.save backup exists" \
        "[ -f ${LOGDIR}/amanda_log.save ]"
}

test_create_amandates() {
    logger "test_create_amandates"
    rm -f ${AMANDATES}
    create_amandates
    assertEquals "create_amandates" 0 $?
    assertTrue "[ -f ${AMANDATES} ]"
}

test_check_amandates() {
    logger "test_check_amandates"
    touch $mock_chown_flags
    touch $mock_chmod_flags
    check_amandates
    assertEquals "check_amandates" 0 $?
    [ "$IAmRoot" ] && { startSkipping; echo "test_check_amandates: skipped"; }
    assertSame \
	"chown args: ${amanda_user}:${amanda_group} ${AMANDATES}" \
	"`cat $mock_chown_flags`"
    assertSame \
	"chmod args: 0640 ${AMANDATES}" \
	"`cat $mock_chmod_flags`"
}

test_create_gnupg() {
    logger "test_create_gnupg"
    create_gnupg
    assertEquals "create_gnupg" 0 $?
    assertTrue "[ -d ${AMANDAHOMEDIR}/.gnupg ]"
}

test_check_gnupg() {
    logger "test_check_gnupg"
    check_gnupg
    assertEquals "check_gnupg" 0 $?
    [ "$IAmRoot" ] && { startSkipping; echo "test_check_gnupg: skipped"; }
    assertSame \
	"chown args: ${amanda_user}:${amanda_group} ${AMANDAHOMEDIR}/.gnupg" \
	"`cat $mock_chown_flags`"
    assertSame \
	"chmod args: 700 ${AMANDAHOMEDIR}/.gnupg" \
	"`cat $mock_chmod_flags`"
}

test_create_amandahosts() {
    logger "test_create_amandahosts"
    create_amandahosts
    assertEquals "create_amandahosts:" 0 $?
    assertEquals "${AMANDAHOMEDIR}/.amandahosts exists:" 0 $?
    assertEquals "create_amandahosts" 0 $?
    assertTrue "[ -f ${AMANDAHOMEDIR}/.amandahosts ]"
}

test_check_amandahosts_entry() {
    logger "test_check_amandahosts_entry"
    if [ -f ${AMANDAHOMEDIR}/.amandahosts ]; then
	check_amandahosts_entry root amindexd amidxtaped
	assertEquals "check_amandahosts_entry root amindexd amidxtaped" \
	    0 $?
    else
	echo "test_check_amandahosts_entry: ${AMANDAHOMEDIR}/.amandahosts missing.  test skipped"
	startSkipping
    fi
}

test_check_amandahosts_perm() {
    logger "test_check_amandahosts_perm"
    check_amandahosts_perms
    assertEquals "check_amandahosts_perms" 0 $?
    [ "$IAmRoot" ] && { startSkipping; echo "test_check_amandahosts_perm: skipped"; }
    assertSame \
	"chown args: ${amanda_user}:${amanda_group} ${AMANDAHOMEDIR}/.amandahosts" \
	"`cat $mock_chown_flags`"
    assertSame \
	"chmod args: 0600 ${AMANDAHOMEDIR}/.amandahosts" \
	"`cat $mock_chmod_flags`"
}

test_create_ssh_key() {
    logger "test_create_ssh_key"
    keydir=${AMANDAHOMEDIR}/.ssh
    rm -rf ${keydir}
    create_ssh_key server
    assertEquals "create_ssh_key" 0 $?
    assertTrue "[ -f ${keydir}/id_rsa_amdump ]"
    [ "$IAmRoot" ] && { startSkipping; echo "test_create_ssh_key: skipped"; }
    assertSame \
	"chown args: ${amanda_user}:${amanda_group} ${keydir} ${keydir}/id_rsa_amdump ${keydir}/id_rsa_amdump.pub" \
	"`cat $mock_chown_flags`"
    # Chmod is called twice, but we only get the 2nd invocation.
    assertSame \
	"chmod args: 0600 ${keydir}/id_rsa_amdump ${keydir}/id_rsa_amdump.pub" \
	"`cat $mock_chmod_flags`"
    endSkipping

    rm -rf ${keydir}
    # What happens if .ssh is a file?
    touch ${AMANDAHOMEDIR}/.ssh
    create_ssh_key client
    assertEquals "create_ssh_key" 0 $?
    assertTrue "[ -f ${keydir}.save ]"
    assertTrue "[ -f ${keydir}/id_rsa_amrecover ]"
}

test_create_profile() {
    logger "test_create_profile"
    rm -f ${AMANDAHOMEDIR}/.profile
    create_profile
    assertEquals "create_profile" 0 $?
    assertTrue "[ -f ${AMANDAHOMEDIR}/.profile ]"
}

test_check_profile() {
    logger "test_check_profile"
    [ -f "${AMANDAHOMEDIR}/.profile" ] || touch ${AMANDAHOMEDIR}/.profile
    check_profile
    assertEquals "check_profile" 0 $?
    assertTrue "[ -s ${AMANDAHOMEDIR}/.profile ]"
    [ "$IAmRoot" ] && { startSkipping; echo "test_check_profile: skipped"; }
    assertSame \
	"chown args: ${amanda_user}:${amanda_group} ${AMANDAHOMEDIR}/.profile" \
	"`cat $mock_chown_flags`"
    assertSame \
	"chmod args: 0640 ${AMANDAHOMEDIR}/.profile" \
	"`cat $mock_chmod_flags`"
}

test_install_client_conf() {
    logger "test_install_client_conf"
    # Test success
    touch ${MOCKDIR}/success
    install_client_conf
    assertEquals "install_client_conf" 0 $?
    prefix="install args:"
    inst_files="${AMANDAHOMEDIR}/example/amanda-client.conf ${SYSCONFDIR}/amanda/"
    case $os in
      SunOS) assertSame \
        "`cat $mock_install_flags`" \
        "${prefix} -m 0600 -u ${amanda_user} -g ${amanda_group} ${inst_files}"
      ;;
      *) assertSame \
        "`cat $mock_install_flags`" \
        "${prefix} -m 0600 -o ${amanda_user} -g ${amanda_group} ${inst_files}"
    esac
}

#TODO: create_ampassphrase, create_amtmp

######################################
#TODO: post_rm_functions

# Run a single test, or let shunit run all tests
if [ $# -gt 0 ]; then
    echo $1
    SPECIFIC_TESTS="$*"
    suite() {
        suite_addTest test___logger
        suite_addTest test__log_output_of
        for t in $SPECIFIC_TESTS; do
            suite_addTest $t
        done
    }
fi

# Importing shunit2 triggers test enumeration, so must happen after
# all tests are defined.
. ${SHUNIT_INC}/shunit2

echo "shunit2 log is: ${LOGFILE}"
echo "mockdir is: ${MOCKDIR}"
