#!/bin/sh

## Mock builtin utilities
# These should preempt builtin programs.
# They should print expected output on STDOUT, and write given arguments to
# mock_<name>_flags.  Be sure to overwrite this file, not append!  If they need
# to act differently depending on a condition, use temp files in $MOCKDIR, and
# delete when you're done.  (Solaris shell does not re-export environment
# variables correctly).

MOCKDIR=$TMPDIR/mocks; export MOCKDIR
[ -d $MOCKDIR ] || mkdir $MOCKDIR
PATH=${MOCKDIR}:$PATH; export PATH
# A list of flag files that must be present to avoid spurious output.
# Each mock util gets one whether it is used or not.
mock_flag_files=""

mk_mock_util() {
    if [ ! "$IAmRoot" ]; then
	# The repetitive stuff
	cat << EOF > "$MOCKDIR/$1"
#!/bin/sh
EOF
	/bin/chmod u+x $MOCKDIR/$1
    else
	touch $MOCKDIR/$1
    fi
    # Dynamic var names are ugly in shell, but useful.
    eval mock_$1_flags=\$MOCKDIR/mock_\$1_flags
    eval export mock_$1_flags
    # Append to the list of flag files
    mock_flag_files="$mock_flag_files mock_$1_flags"
}

###############
# ps replacement
mk_mock_util "ps"
cat << EOF >> "${MOCKDIR}/ps"
echo "ps args: \${@}" > $mock_ps_flags
if [ -f "${MOCKDIR}/running" ]; then
    ps_list="inetd xinetd launchd"
else
    ps_list="foo bar baz"
fi

echo "faux process list: \$ps_list"
EOF

###############
# chown replacement
mk_mock_util "chown"
cat << EOF >> "${MOCKDIR}/chown"
echo "chown args: \${@}" > $mock_chown_flags
EOF

###############
# chmod replacement
mk_mock_util "chmod"
cat << EOF >> "${MOCKDIR}/chmod"
echo "chmod args: \${@}" > $mock_chmod_flags
EOF

###############
# id replacemnt

# Set vars for output of -Gn
id_0="biff"
id_1="biff foo"
id_2="biff bar baz"
id_3="biff zip bop whir"

# Set defaults for uid and amanda_group, if running outside test_sh_libs
deb_uid=${deb_uid:=12345}
amanda_group=${amanda_group:=disk}

mk_mock_util "id"
cat << EOF >> "${MOCKDIR}/id"
echo "id args: \${@}" > $mock_id_flags
[ -n "\${1}" ] || { echo "Missing a username to id!"; exit 1; }

# We can only use id with no flags to have consistent results.  Solaris
# /usr/bin/id does not provide any standard flags. Since /usr/xpg4/bin is
# not part of a minimal install we can't depend on it. Any flags *at all*
# should raise an error in the tests.
# group file and /usr/bin/groups can be used (with some tweaks).
for f in "\$@"; do
    case \$f in
        # -- is ok, surprisingly.
        --) : ;;
        -?*) echo "id: no options are portable! '\$f'"
             # Solaris exits with 2, others with 1. ugh.
             exit 2
        ;;
        *) : ;;
    esac
done

# Use id_group to control primary group name.
[ -f ${MOCKDIR}/id_group ] && group=\`cat ${MOCKDIR}/id_group\` || group=bar

# Solaris, Linux and OSX differ in the exact format of the output from id,
# so we provide sample output based on the contents of id_os.  We don't
# parse it, but its presence can't break things.
test_os=\`cat ${MOCKDIR}/id_os\`
case \${test_os} in
    linux) sup_groups=" groups=999(\${group}),1000(foo)" ;;
    solaris) sup_groups="" ;;
    osx) sup_groups=" groups=999(\${group}), 1000(foo)" ;;
esac

if [ -f ${MOCKDIR}/id_exists ]; then
    # Note: uid is set when the mock is created.
    echo "uid=${deb_uid}(\${1}) gid=6(\${group})\${sup_groups}"
else
    echo "id: \${1}: no such user" >&2
    exit 1
fi
EOF

###############
# groupadd replacement
mk_mock_util "groupadd"
cat << EOF >> "${MOCKDIR}/groupadd"
echo "groupadd args: \${@}" > $mock_groupadd_flags
# We check for return codes of 0 (group added) or 9 (group existed) to
# continue in the function that uses groupadd
groupadd_rc=\`cat ${MOCKDIR}/groupadd_rc\`
exit \${groupadd_rc}
EOF

###############
# groups replacement
mk_mock_util "groups"
cat << EOF >> "${MOCKDIR}/groups"
echo "groups args: \${@}" > $mock_groups_flags
cat ${MOCKDIR}/groups_output
EOF

###############
# usermod replacement
mk_mock_util "usermod"
cat << EOF >> "${MOCKDIR}/usermod"
echo "usermod args: \${@}" > $mock_usermod_flags
# Protect against passing a blank username.
[ "x\${1}" = "x" ] && exit 2 || exit 0
EOF

###############
# svcs replacement
mk_mock_util "svcs"
cat << EOF >> "${MOCKDIR}/svcs"
echo "svcs args: \${@}" > $mock_svcs_flags
if [ -f "${MOCKDIR}/prexisting_service" ]; then
    echo "online         Sep_09   svc:/network/amanda/tcp:default"
else
    echo "Pattern '\${3}' doesn't match any instances"
    exit 1
fi
EOF

###############
# xinetd init script replacement
mk_mock_util xinetd
cat << EOF >> "${MOCKDIR}/xinetd"
echo "xinetd args: \${@}" > $mock_xinetd_flags
[ -f ${MOCKDIR}/success ] && exit 0
echo "xinetd did not \${1}"
exit 1
EOF

###############
# inetd init script replacement
mk_mock_util inetd
cat << EOF >> "${MOCKDIR}/inetd"
echo "inetd args: \${@}" > $mock_inetd_flags
[ -f ${MOCKDIR}/success ] && exit 0
echo "inetd did not \${1}"
exit 1
EOF

###############
# install replacement
mk_mock_util install
cat << EOF >> "${MOCKDIR}/install"
echo "install args: \${@}" > $mock_install_flags
[ -f ${MOCKDIR}/success ] && exit 0
echo "Some funky install error, yo!"
exit 1
EOF

# TODO: inetconv inetadm, svcadm

# Touch all the flag files
for file in $mock_flag_files; do
    touch $MOCKDIR/$file
done

