# SYNOPSIS
#
#   AMANDA_KRB5_SECURITY
#
# OVERVIEW
#
#   Handle configuration for KRB5 security, implementing the --with-krb5-security
#   option.  If libraries are found, they are added to the relevant compiler flags.
#
#   Defines and substitutes KRB5_SECURITY, and sets AM_CONDITIONAL 
#   WANT_KRB5_SECURITY, if the user has selected this mechanism.  Also, the
#   following parameters are taken from options, defined, and substituted:
#
AC_DEFUN([AMANDA_KRB5_SECURITY],
[
    # Specify --with-krb5-security if Kerberos software is in somewhere
    # other than the listed KRB5_SPOTS.  We only compile kerberos support in
    # if the right files are there.

    KRB5_SECURITY="no"
    : ${KRB5_SPOTS="/usr/kerberos /usr/cygnus /usr /opt/kerberos"}

    AC_ARG_WITH(krb5-security,
        AS_HELP_STRING([--with-krb5-security=DIR],
            [Location of Kerberos V software @<:@/usr/kerberos /usr/cygnus /usr /opt/kerberos@:>@]),
        [
            case "$withval" in
                n | no) KRB5_SECURITY=no ;;
                y | ye | yes) KRB5_SECURITY=yes ;;
                *) KRB5_SPOTS="$KRB5_SECURITY"
                   KRB5_SECURITY=yes
                   ;;
            esac
        ]
    )

    if test "x$KRB5_SECURITY" = "xyes"; then
        # if found, force the static versions of these libs (.a) by linking directly
        # with the .a files.  I don't know how to get -R dependancies checked
        # in autoconf at this time. -kashmir
        AC_MSG_CHECKING(for Kerberos V libraries)
        KRB5_DIR_FOUND=""
        for dir in $KRB5_SPOTS; do
          for lib in lib lib64; do
            k5libdir=${dir}/${lib}
            if test \( -f ${k5libdir}/libkrb5.a -o -f ${k5libdir}/libkrb5.so \) -a \( -f ${k5libdir}/libgssapi_krb5.so -o -f ${k5libdir}/libgssapi_krb5.a \) -a \( -f ${k5libdir}/libcom_err.a -o -f ${k5libdir}/libcom_err.so \); then
                if test -f ${k5libdir}/libk5crypto.a -o -f ${k5libdir}/libk5crypto.so; then
                    K5CRYPTO=-lk5crypto
                elif test -f ${k5libdir}/libcrypto.a -o -f ${k5libdir}/libcrypto.so; then
                    K5CRYPTO=-lcrypto
                else
                    K5CRYPTO=""
                fi
                if test -f ${k5libdir}/libkrb5support.a -o -f ${k5libdir}/libkrb5support.so; then
                    K5SUPPORT=-lkrb5support
                else
                    K5SUPPORT=""
                fi
                KRB5_DIR_FOUND=$dir
                KRB5_LIBDIR_FOUND=$k5libdir
                AMANDA_ADD_LIBS([-lgssapi_krb5 -lkrb5 $K5CRYPTO $K5SUPPORT -lcom_err])
                break
            elif test \( -f ${k5libdir}/libkrb5.a -o -f ${k5libdir}/libkrb5.so \) -a \( -f ${k5libdir}/libasn1.a -o -f ${k5libdir}/libasn1.so \) -a \( -f ${k5libdir}/libgssapi.a -o -f ${k5libdir}/libgssapi.so \); then
                AMANDA_ADD_LIBS([-lgssapi -lkrb5 -lasn1])
                AMANDA_ADD_CPPFLAGS([-DKRB5_HEIMDAL_INCLUDES])
                break
            fi
          done
        done

        if test "$KRB5_DIR_FOUND"; then
            AC_MSG_RESULT(found in $KRB5_DIR_FOUND)
            #
            # some OS's, such as NetBSD, stick krb5 includes out of the way...
            # should probably just use autoconf to look for various include
            # options and set them, but don't quite want to do that until I've
            # dug into it a bit more.
            #
            if test -d "$KRB5_DIR_FOUND/krb5" ; then
                AMANDA_ADD_CPPFLAGS([-I$KRB5_DIR_FOUND/include/krb5])
            else
                AMANDA_ADD_CPPFLAGS([-I$KRB5_DIR_FOUND/include])
            fi
                AC_CHECK_LIB(krb5support,main)
            AMANDA_ADD_LDFLAGS([-L$KRB5_LIBDIR_FOUND])

            AC_DEFINE(KRB5_SECURITY,1,
                [Define if Kerberos 5 security is to be enabled. ])
        else
            AC_MSG_RESULT(no krb5 system libraries found)
            AC_MSG_ERROR([No Kerberos V libraries were found on your system; krb5-security cannot be enabled])
            KRB5_SECURITY="no"
        fi
    fi

    AM_CONDITIONAL(WANT_KRB5_SECURITY, test x"$KRB5_SECURITY" = x"yes")
    AC_SUBST(KRB5_SECURITY)
])
