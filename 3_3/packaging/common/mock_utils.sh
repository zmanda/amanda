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

# Set defaults for deb_uid and amanda_group, if running outside test_sh_libs
deb_uid=${deb_uid:=12345}
amanda_group=${amanda_group:=disk}

mk_mock_util "id"
cat << EOF >> "${MOCKDIR}/id"
echo "id args: \${1}" > $mock_id_flags
[ -n "\${1}" ] || { echo "Missing a username to id!"; exit 1; }

# We have to return the most basic form of id to be portable
# group file is used for supplemental groups.
if [ -f "${MOCKDIR}/is_member" ]; then
    echo "uid=${deb_uid}(\${1}) gid=6(${amanda_group})"
else
    echo "uid=123(\${1}) gid=123(foobar)"
fi
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
echo "xinetd args: \${1}" > $mock_xinetd_flags
[ -f ${MOCKDIR}/success ] && exit 0
echo "xinetd did not \${1}"
exit 1
EOF

###############
# inetd init script replacement
mk_mock_util inetd
cat << EOF >> "${MOCKDIR}/inetd"
echo "inetd args: \${1}" > $mock_inetd_flags
[ -f ${MOCKDIR}/success ] && exit 0
echo "inetd did not \${1}"
exit 1
EOF

###############
# install replacement
mk_mock_util install
cat << EOF >> "${MOCKDIR}/install"
echo "install args: \$@" > $mock_install_flags
[ -f ${MOCKDIR}/success ] && exit 0
echo "Some funky install error, yo!"
exit 1
EOF

# TODO: inetconv inetadm, svcadm

# Touch all the flag files
for file in $mock_flag_files; do
    touch $MOCKDIR/$file
done

