# SYNOPSIS
#
#   AMANDA_KRB4_SECURITY
#
# OVERVIEW
#
#   Handle configuration for KRB4 security, implementing the --with-krb4-security
#   option.  If libraries are found, they are added to the relevant compiler flags.
#
#   Defines and substitutes KRB4_SECURITY, and sets AM_CONDITIONAL WANT_KRB4_SECURITY,
#   if the user has selected this mechanism.  Also, the following parameters
#   are taken from options, defined, and substituted:
#
#    - SERVER_HOST_PRINCIPAL
#    - SERVER_HOST_INSTANCE
#    - SERVER_HOST_KEY_FILE
#    - CLIENT_HOST_PRINCIPAL
#    - CLIENT_HOST_INSTANCE
#    - CLIENT_HOST_KEY_FILE
#    - TICKET_LIFETIME
#
AC_DEFUN([AMANDA_KRB4_SECURITY],
[
    # Specify --with-krb4-security if Kerberos software is in somewhere
    # other than the listed KRB4_SPOTS.  We only compile kerberos support in
    # if the right files are there.

    : ${KRB4_SPOTS="/usr/kerberos /usr/cygnus /usr /opt/kerberos"}

    KRB4_SECURITY="no"
    AC_ARG_WITH(krb4-security,
        AS_HELP_STRING([--with-krb4-security=DIR],
            [Location of Kerberos software @<:@/usr/kerberos /usr/cygnus /usr /opt/kerberos@:>@]),
        [
            case "$withval" in
                n | no) ;;
                y | ye | yes) KRB4_SECURITY="yes" ;;
                *) KRB4_SPOTS="$KRB4_SECURITY"
                   KRB4_SECURITY="yes"
                   ;;
            esac
        ],
    )

    # check the remaining, subsidiary options

    AC_MSG_CHECKING([host principal])
    AC_ARG_WITH(server-principal,
        AS_HELP_STRING([ --with-server-principal=ARG],
            [server host principal ("amanda")]),
        [
            case "$withval" in
                "" | y | ye | yes | n | no)
                    AC_MSG_ERROR([*** You must supply an argument to the --with-server-principal option.])
                  ;;
                *) SERVER_HOST_PRINCIPAL="$withval" ;;
            esac
        ],
        [ : ${SERVER_HOST_PRINCIPAL="amanda"} ]
    )
    AC_MSG_RESULT($SERVER_HOST_PRINCIPAL)

    AC_MSG_CHECKING([server host instance])
    AC_ARG_WITH(server-instance,
        AS_HELP_STRING([ --with-server-instance=ARG],
            [server host instance ("amanda")]),
        [
            case "$withval" in
                "" | y | ye | yes | n | no)
                    AC_MSG_ERROR([*** You must supply an argument to the --with-server-instance option.])
                  ;;
                *) SERVER_HOST_INSTANCE="$withval" ;;
            esac
        ],
        [ : ${SERVER_HOST_INSTANCE="amanda"} ]
    )
    AC_MSG_RESULT($SERVER_HOST_INSTANCE)

    AC_MSG_CHECKING([server host key file])
    AC_ARG_WITH(server-keyfile,
        AS_HELP_STRING([ --with-server-keyfile=ARG],
            [server host key file ("/.amanda")]),
        [
            case "$withval" in
                "" | y | ye | yes | n | no)
                    AC_MSG_ERROR([*** You must supply an argument to the --with-server-keyfile option.])
                  ;;
                *) SERVER_HOST_KEY_FILE="$withval" ;;
            esac
        ],
        [ : ${SERVER_HOST_KEY_FILE="/.amanda"} ]
    )
    AC_MSG_RESULT($SERVER_HOST_KEY_FILE)

    AC_MSG_CHECKING(client host principle)
    AC_ARG_WITH(client-principal,
        AS_HELP_STRING([ --with-client-principal=ARG],
            [client host principle ("rcmd")]),
        [
            case "$withval" in
                "" | y | ye | yes | n | no)
                    AC_MSG_ERROR([*** You must supply an argument to the --with-client-principal option.])
                  ;;
                *) CLIENT_HOST_PRINCIPAL="$withval" ;;
            esac
        ],
        [ : ${CLIENT_HOST_PRINCIPAL="rcmd"} ]
    )
    AC_MSG_RESULT($CLIENT_HOST_PRINCIPAL)

    AC_MSG_CHECKING([client host instance])
    AC_ARG_WITH(client-instance,
        AS_HELP_STRING([ --with-client-instance=ARG],
            [client host instance (HOSTNAME_INSTANCE)]),
        [
            case "$withval" in
                "" | y | ye | yes | n | no)
                    AC_MSG_ERROR([*** You must supply an argument to the --with-client-instance option.])
                  ;;
                *) CLIENT_HOST_INSTANCE="$withval" ;;
            esac
        ],
        [ : ${CLIENT_HOST_INSTANCE=HOSTNAME_INSTANCE} ]
    )
    AC_MSG_RESULT($CLIENT_HOST_INSTANCE)

    AC_MSG_CHECKING([client host key file])
    AC_ARG_WITH(client-keyfile,
        AS_HELP_STRING([ --with-client-keyfile=ARG],
            [client host key file (KEYFILE)]),
        [
            case "$withval" in
                "" | y | ye | yes | n | no)
                    AC_MSG_ERROR([*** You must supply an argument to the --with-client-keyfile option.])
                  ;;
                *) CLIENT_HOST_KEY_FILE="$withval" ;;
            esac
        ],
        [ : ${CLIENT_HOST_KEY_FILE=KEYFILE} ]
    )
    # Assume it's either KEYFILE (defined in krb.h), or a string filename...
    if test "x$CLIENT_HOST_KEY_FILE" != "xKEYFILE"; then
      # add quotes
      CLIENT_HOST_KEY_FILE="\"$CLIENT_HOST_KEY_FILE\""
    fi
    AC_MSG_RESULT($CLIENT_HOST_KEY_FILE)

    AC_MSG_CHECKING([ticket lifetime])
    AC_ARG_WITH(ticket-lifetime,
        AS_HELP_STRING([ --ticket-lifetime],
            [ticket lifetime (128)]),
        [
            case "$withval" in
            "" | y | ye | yes | n | no)
                AC_MSG_ERROR([*** You must supply an argument to the --with-ticket-lifetime option.])
              ;;
            *) TICKET_LIFETIME="$withval" ;;
            esac
        ],
        [ : ${TICKET_LIFETIME=128} ]
    )
    AC_MSG_RESULT($TICKET_LIFETIME)


    if test "x${KRB4_SECURITY}" = "xyes"; then
        AC_MSG_CHECKING(for Kerberos and Amanda kerberos4 bits)
        found="no"
        for dir in $KRB4_SPOTS; do
            if test \( -f ${dir}/lib/libkrb.a -o -f ${dir}/lib/libkrb.so \) -a \( -f ${dir}/lib/libdes.a -o -f ${dir}/lib/libdes.so \) ; then
                #
                # This is the original Kerberos 4.
                #
                AC_MSG_RESULT(found in $dir)
                found="yes"

                #
                # This handles BSD/OS.
                #
                if test -d $dir/include/kerberosIV ; then
                    AMANDA_ADD_CPPFLAGS([-I$dir/include/kerberosIV])
                else
                    AMANDA_ADD_CPPFLAGS([-I$dir/include])
                fi
                AMANDA_ADD_LDFLAGS([-L$dir/lib])
                AMANDA_ADD_LIBS([-lkrb -ldes])
                if test -f ${dir}/lib/libcom_err.a; then
                    AMANDA_ADD_LIBS([-lcom_err])
                fi
                break
            elif test \( -f ${dir}/lib/libkrb4.a -o -f ${dir}/lib/libkrb4.so \) &&
                 test \( -f ${dir}/lib/libcrypto.a -o -f ${dir}/lib/libcrypto.so \) &&
                 test \( -f ${dir}/lib/libdes425.a -o -f ${dir}/lib/libdes425.so \) ; then
                #
                # This is Kerberos 5 with Kerberos 4 back-support.
                #
                AC_MSG_RESULT(found in $dir)
                found="yes"
                AMANDA_ADD_CPPFLAGS([-I$dir/include -I$dir/include/kerberosIV])
                AMANDA_ADD_LDFLAGS([-L$dir/lib])
                if test \( -f ${dir}/lib/libkrb5.a -o -f ${dir}/lib/libkrb5.so \) &&
                   test \( -f ${dir}/lib/libcom_err.a -o -f ${dir}/lib/libcom_err.so \) ; then
                    AMANDA_ADD_LIBS([-lkrb4 -lkrb5 -lcrypto -ldes425 -lcom_err])
                else
                    AMANDA_ADD_LIBS([-lkrb4 -lcrypto -ldes425])
                fi
                break
            fi
        done

        if test "x$found" = "xno" ; then
            AC_MSG_RESULT(no libraries found)
            AMANDA_MSG_WARN([No Kerberos IV libraries were found on your system; disabling krb4-security])
            KRB4_SECURITY="no"
        else
            AC_DEFINE(KRB4_SECURITY, 1, 
                [Enable Kerberos IV security.])
            AC_DEFINE_UNQUOTED(SERVER_HOST_PRINCIPAL,"$SERVER_HOST_PRINCIPAL",
                    [The Kerberos server principal. ])
            AC_DEFINE_UNQUOTED(SERVER_HOST_INSTANCE,"$SERVER_HOST_INSTANCE",
                    [The Kerberos server instance. ])
            AC_DEFINE_UNQUOTED(SERVER_HOST_KEY_FILE,"$SERVER_HOST_KEY_FILE",
                    [The Kerberos server key file. ])
            AC_DEFINE_UNQUOTED(CLIENT_HOST_PRINCIPAL,"$CLIENT_HOST_PRINCIPAL",
                    [The Kerberos client host principal. ])
            AC_DEFINE_UNQUOTED(CLIENT_HOST_INSTANCE,$CLIENT_HOST_INSTANCE,
                    [The Kerberos client host instance. ])
            AC_DEFINE_UNQUOTED(CLIENT_HOST_KEY_FILE,$CLIENT_HOST_KEY_FILE,
                    [The Kerberos client host key file. ])
            AC_DEFINE_UNQUOTED(TICKET_LIFETIME,$TICKET_LIFETIME,
                    [The Kerberos ticket lifetime. ])
        fi
    fi
    AM_CONDITIONAL(WANT_KRB4_SECURITY, test x"$KRB4_SECURITY" = x"yes")

    AC_SUBST(KRB4_SECURITY)

    AC_SUBST(SERVER_HOST_PRINCIPAL)
    AC_SUBST(SERVER_HOST_INSTANCE)
    AC_SUBST(SERVER_HOST_KEY_FILE)
    AC_SUBST(CLIENT_HOST_PRINCIPAL)
    AC_SUBST(CLIENT_HOST_INSTANCE)
    AC_SUBST(CLIENT_HOST_KEY_FILE)
    AC_SUBST(TICKET_LIFETIME)
])
