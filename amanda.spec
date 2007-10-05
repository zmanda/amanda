#
#                  Copyright (C) 2005 Zmanda Incorporated.
#                            All Rights Reserved.
#
#  This program is free software; you can redistribute it and/or modify it
#  under the terms of the GNU General Public License version 2 as published
#  by the Free Software Foundation.
# 
#  This program is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
#  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
#  for more details.
# 
#  You should have received a copy of the GNU General Public License along
#  with this program; if not, write to the Free Software Foundation, Inc.,
#  59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
# 
#  Contact information: Zmanda Inc, 505 N Mathlida Ave, Suite 120
#  Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
#

# Define amanda_version if it is not already defined.
%{!?amanda_version: %define amanda_version 2.5.3alpha}
%{!?amanda_release: %define amanda_release 1}

%define build_srpm 0
%{?srpm_only: %define build_srpm 1}

# Define which Distribution we are building:
%define is_rhel3   0
%define is_rhel4   0
%define is_rhel5   0
%define is_fedora3 0
%define is_fedora4 0
%define is_fedora5 0
%define is_fedora6 0
%define is_fedora7 0
%define is_suse10  0
%define is_sles9  0
%define is_sles10  0
%{?build_rhel3:   %define is_rhel3   1}
%{?build_rhel4:   %define is_rhel4   1}
%{?build_rhel5:   %define is_rhel5   1}
%{?build_fedora3: %define is_fedora3 1}
%{?build_fedora4: %define is_fedora4 1}
%{?build_fedora5: %define is_fedora5 1}
%{?build_fedora6: %define is_fedora6 1}
%{?build_fedora7: %define is_fedora7 1}
%{?build_suse10:  %define is_suse10  1}
%{?build_sles9:   %define is_sles9   1}
%{?build_sles10:  %define is_sles10   1}

%if ! %{is_rhel3} && ! %{is_rhel4} && ! %{is_rhel5} && ! %{is_fedora3} && ! %{is_fedora4} && ! %{is_fedora5} && ! %{is_fedora6} && ! %{is_fedora7} && ! %{is_suse10} && ! %{is_sles9} && ! %{is_sles10}
%(error: Please specify which distribution to build RPMS for.)
exit 1
%endif

# Set options per distribution
%if %{is_rhel3} || %{is_rhel4} || %{is_rhel5}
%define dist redhat
%define disttag rhel
%define build_host i386-redhat-linux
%define distver %(cat /etc/redhat-release | awk '{split($_,v); print v[7]}')
%define rpm_group Applications/Archiving
%endif
%if %{is_fedora3}
%define dist fedora
%define disttag fc
%define build_host i386-redhat-linux
%define distver %(release="`rpm -q --queryformat='%{VERSION}' %{dist}-release 2> /dev/null | tr . : | sed s/://g`" ; if test $? != 0 ; then release="" ; fi ; echo "$release")
%define rpm_group Applications/Archiving
%endif
%if %{is_fedora4}
%define dist fedora
%define disttag fc
%define build_host i386-redhat-linux
%define distver %(release="`rpm -q --queryformat='%{VERSION}' %{dist}-release 2> /dev/null | tr . : | sed s/://g`" ; if test $? != 0 ; then release="" ; fi ; echo "$release")
%define rpm_group Applications/Archiving
%endif
%if %{is_fedora5}
%define dist fedora
%define disttag fc
%define build_host i386-redhat-linux
%define distver %(release="`rpm -q --queryformat='%{VERSION}' %{dist}-release 2> /dev/null | tr . : | sed s/://g`" ; if test $? != 0 ; then release="" ; fi ; echo "$release")
%define rpm_group Applications/Archiving
%endif
%if %{is_fedora6}
%define dist fedora
%define disttag fc
%define build_host i386-redhat-linux
%define distver %(release="`rpm -q --queryformat='%{VERSION}' %{dist}-release 2> /dev/null | tr . : | sed s/://g`" ; if test $? != 0 ; then release="" ; fi ; echo "$release")
%define rpm_group Applications/Archiving
%endif
%if %{is_fedora7}
%define dist fedora
%define disttag fc
%define build_host i386-redhat-linux
%define distver %(release="`rpm -q --queryformat='%{VERSION}' %{dist}-release 2> /dev/null | tr . : | sed s/://g`" ; if test $? != 0 ; then release="" ; fi ; echo "$release")
%define rpm_group Applications/Archiving
%endif
%if %{is_suse10}
%define dist SuSE
%define disttag suse
%define build_host i586-suse-linux
%define distver %(cat /etc/SuSE-release | grep VERSION | awk '{split($_,v); print v[3]}')
%define rpm_group Productivity/Archiving/Backup
%endif
%if %{is_sles9} || %{is_sles10}
%define dist SuSE
%define disttag sles
%define build_host i586-suse-linux
%define distver %(cat /etc/SuSE-release | grep VERSION | awk '{split($_,v); print v[3]}')
%define rpm_group Productivity/Archiving/Backup
%endif

%define packer %(%{__id_u} -n)

# --- Definitions ---

%define amanda_version_info "Amanda Community Edition - version %{amanda_version}"
%define amanda_user amandabackup
%define amanda_group disk

Summary: The Amanda Backup and Archiving System
Name: amanda
Version: %{amanda_version}
%define rpm_release %{amanda_release}.%{disttag}%{distver}
%if %{build_srpm}
%define rpm_release %{amanda_release}
%endif
Release: %{rpm_release}
Source: %{name}-%{version}.tar.gz
Patch0: community-rpm-Makefiles.patch.gz
License: http://wiki.zmanda.com/index.php/Amanda_Copyright
Vendor: Zmanda, Inc.
Packager: www.zmanda.com
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-%{packer}-buildroot
Group: %{rpm_group}
Prereq: /bin/date
Prereq: /bin/sh
Prereq: /sbin/ldconfig
Prereq: /usr/bin/id
Prereq: /usr/sbin/useradd
Prereq: /usr/sbin/usermod
# TODO - Need required versions for these:
BuildRequires: autoconf
BuildRequires: automake
BuildRequires: binutils
BuildRequires: bison
BuildRequires: flex
BuildRequires: gcc
BuildRequires: glibc
BuildRequires: gnuplot
BuildRequires: readline
Requires: /bin/awk
Requires: fileutils
Requires: grep
Requires: libc.so.6
Requires: libm.so.6
Requires: libnsl.so.1
%if  %{is_rhel3} || %{is_rhel4} || %{is_rhel5} || %{is_fedora3} || %{is_fedora4} || %{is_fedora5} || %{is_fedora6} || %{is_fedora7}
Requires: libtermcap.so.2
Requires: initscripts
%endif
Requires: xinetd
Requires: perl
Requires: tar 
Provides: amanda-backup_client, amanda-backup_server

%package backup_client
Summary: The Amanda Backup and Archiving Client
Group: %{rpm_group}
Requires: /bin/awk
Requires: fileutils
Requires: grep
Requires: libc.so.6
Requires: libm.so.6
Requires: libnsl.so.1
%if  %{is_rhel3} || %{is_rhel4} || %{is_rhel5} || %{is_fedora3} || %{is_fedora4} || %{is_fedora5} || %{is_fedora6} || %{is_fedora7}
Requires: libtermcap.so.2
Requires: initscripts
%endif
Requires: xinetd
Requires: perl
Requires: tar
Provides: amanda-backup_client
Provides: libamclient-%{version}.so
Provides: libamanda-%{version}.so
Provides: librestore-%{version}.so
Conflicts: amanda-backup_server

%package backup_server
Summary: The Amanda Backup and Archiving Server
Group: %{rpm_group}
Requires: /bin/awk
Requires: fileutils
Requires: grep
Requires: libc.so.6
Requires: libm.so.6
Requires: libnsl.so.1
%if  %{is_rhel3} || %{is_rhel4} || %{is_rhel5} || %{is_fedora3} || %{is_fedora4} || %{is_fedora5} || %{is_fedora6} || %{is_fedora7}
Requires: libtermcap.so.2
Requires: initscripts
%endif
Requires: xinetd
Requires: perl
Requires: tar
Provides: amanda-backup_server
Provides: libamclient-%{version}.so
Provides: libamanda-%{version}.so
Provides: libamserver-%{version}.so
Provides: librestore-%{version}.so
Provides: libamtape-%{version}.so

# --- Package descriptions ---

%description
Amanda is the leading Open-Source Backup and Archiving software.

The amanda-backup_server package should be installed on the Amanda server, i.e. 
the machine attached to backup media (such as a tape drive or disk 
drives) where backups will be written. The amanda-backup_server package
includes Amanda client.  The amanda-backup_client package needs 
to be installed on every system that is being backed up.

Amanda Forums is located at: http://forums.zmanda.com/
Amanda Documentation is available at: http://wiki.zmanda.com/



%description backup_server
Amanda is the leading Open-Source Backup and Archiving software.

This package contains the Amanda server.  The amanda-backup_server package 
should be installed on the Amanda server, i.e. the machine attached 
to backup media (such as a tape drive or disk drives) where backups 
will be written.  The amanda-backup_server package includes Amanda client.

Amanda Forums is located at: http://forums.zmanda.com/
Amanda Documentation is available at: http://wiki.zmanda.com/



%description backup_client
Amanda is the leading Open-Source Backup and Archiving software.

This package contains the Amanda client.  The amanda-backup_client package  
needs to be installed on every system that is being backed up.

Amanda Forums is located at: http://forums.zmanda.com/
Amanda Documentation is available at: http://wiki.zmanda.com/

# --- Directory setup ---

# Configure directories:
%define PREFIX		/usr
%define EPREFIX		%{PREFIX}
%define BINDIR		%{EPREFIX}/bin
%define SBINDIR		%{EPREFIX}/sbin
%define LIBEXECDIR	%{EPREFIX}/lib/amanda
%define DATADIR		%{PREFIX}/share
%define SYSCONFDIR	/etc
%define LOCALSTATEDIR	/var/lib/amanda
%define LIBDIR		%{EPREFIX}/lib
%define INCLUDEDIR	%{PREFIX}/include
%define INFODIR		%{PREFIX}/info
%define MANDIR		%{DATADIR}/man
%define LOGDIR		/var/log/amanda

# Installation directories:
%define ROOT_SBINDIR		%{buildroot}/%{SBINDIR}
%define ROOT_LIBEXECDIR		%{buildroot}/%{LIBEXECDIR}
%define ROOT_DATADIR		%{buildroot}/%{DATADIR}
%define ROOT_SYSCONFDIR		%{buildroot}/%{SYSCONFDIR}
%define ROOT_LOCALSTATEDIR	%{buildroot}/%{LOCALSTATEDIR}
%define ROOT_LIBDIR		%{buildroot}/%{LIBDIR}
%define ROOT_INFODIR		%{buildroot}/%{INFODIR}
%define ROOT_MANDIR		%{buildroot}/%{MANDIR}
%define ROOT_LOGDIR		%{buildroot}/%{LOGDIR}

# --- Unpack and apply patches (if any) ---

%prep
%setup
%patch -P 0 -p1

# --- Configure and compile ---

%build
%define config_user %{amanda_user}
%define config_group %{amanda_group}

CFLAGS="%{optflags} -m32 -g" CXXFLAGS="%{optflags} -m32" \
./configure \
	--build=%{build_host} \
	--prefix=%{PREFIX} \
	--bindir=%{BINDIR} \
	--sbindir=%{SBINDIR} \
	--libexecdir=%{LIBEXECDIR} \
	--datadir=%{DATADIR} \
	--sysconfdir=%{SYSCONFDIR} \
	--sharedstatedir=%{LOCALSTATEDIR} \
	--localstatedir=%{LOCALSTATEDIR} \
	--libdir=%{LIBDIR} \
	--includedir=%{INCLUDEDIR} \
	--infodir=%{INFODIR} \
	--mandir=%{MANDIR} \
	--with-gnutar=/bin/tar \
	--with-gnutar-listdir=%{LOCALSTATEDIR}/gnutar-lists \
	--with-dumperdir=%{LIBEXECDIR} \
	--with-index-server=localhost \
	--with-tape-server=localhost \
	--with-user=%{config_user} \
	--with-group=%{config_group} \
	--with-owner=%{packer} \
	--with-fqdn \
	--with-bsd-security \
	--with-bsdtcp-security \
	--with-bsdudp-security \
	--with-debugging=%{LOGDIR} \
	--with-ssh-security \
        --with-assertions

make

# --- Install to buildroot ---

%install
if [ "%{buildroot}" != "/" ]; then
	if [ -d "%{buildroot}" ]; then
		rm -rf %{buildroot}
	fi
else
	echo "BuildRoot was somehow set to / !"
	exit -1
fi

make DESTDIR=%{buildroot} install

rm -rf %{ROOT_DATADIR}/amanda
rm -f %{ROOT_LOCALSTATEDIR}/example/inetd.conf.amandaclient
mkdir %{buildroot}/{etc,var/,var/log,var/lib,var/lib/amanda,var/lib/amanda/example,var/lib/amanda/example/label-templates}
mkdir %{ROOT_SYSCONFDIR}/amanda
mkdir %{ROOT_LOCALSTATEDIR}/gnutar-lists
mkdir %{ROOT_LOGDIR}
cp example/amanda.conf %{ROOT_LOCALSTATEDIR}/example/amanda.conf
cp example/amanda-client.conf %{ROOT_LOCALSTATEDIR}/example/amanda-client.conf
cp example/DLT.ps %{ROOT_LOCALSTATEDIR}/example/label-templates/
cp example/EXB-8500.ps %{ROOT_LOCALSTATEDIR}/example/label-templates/
cp example/HP-DAT.ps %{ROOT_LOCALSTATEDIR}/example/label-templates/
cp example/8.5x11.ps %{ROOT_LOCALSTATEDIR}/example/label-templates/
cp example/3hole.ps %{ROOT_LOCALSTATEDIR}/example/label-templates/
cp example/DIN-A4.ps %{ROOT_LOCALSTATEDIR}/example/label-templates/

echo "%{amanda_version_info}" >%{ROOT_LOCALSTATEDIR}/amanda-release

# --- Clean up buildroot ---

%clean
if [ "%{buildroot}" != "/" ]; then
	if [ -d %{buildroot} ]; then
		rm -rf %{buildroot}
	fi
else
	echo "BuildRoot was somehow set to / !"
	exit -1
fi

# --- Pre/post (un)installation scripts ---

%pre
TMPFILE=`mktemp /tmp/rpm-amanda.XXXXXXXXXXX`
if [ $? -ne 0 ]; then
	echo "Unable to mktemp!" 1>&2
	exit 1
fi
LOGDIR="%{LOGDIR}"
INSTALL_LOG="${LOGDIR}/install.log"
INSTALL_ERR="${LOGDIR}/install.err"

echo "`date +'%b %e %Y %T'`: Preparing to install: %{amanda_version_info}" >${TMPFILE}

# Check for the 'amanda' user
echo "`date +'%b %e %Y %T'`: Checking for '%{amanda_user}' user..." >>${TMPFILE}
if [ "`id -u %{amanda_user} >&/dev/null && echo 0 || echo 1`" != "0" ] ; then
	useradd -c "Amanda" -M -g disk -d %{LOCALSTATEDIR} -s /bin/sh %{amanda_user}
	if [ %{is_suse10} -eq 1 ] || [ %{is_sles9} -eq 1 ] || [ %{is_sles10} -eq 1 ]; then
		PASSWD_EXIT=$?
	else
		# Lock the amanda account until admin sets password
		passwd -l %{amanda_user} >>/dev/null
		PASSWD_EXIT=$?
	fi
	if [ ${PASSWD_EXIT} -eq 0 ] ; then
		echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  The '%{amanda_user}; user account has been successfully created." >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  Furthermore, the account has been automatically locked for you" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  for security purposes.  Once a password for the  '%{amanda_user}'" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  account has been set, the user can be unlocked by issuing" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  the following command as root.:" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  # passwd -u %{amanda_user}" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  If this is not a new installation of Amanda and you have" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  pre-existing Amanda configurations in %{SYSCONFDIR}/amanda" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  you should ensure that 'dumpuser' is set to '%{amanda_user}'" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  in those configurations.  Additionally, you should ensure" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  that %{LOCALSTATEDIR}/.amandahosts on your client systems" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  is properly configured to allow connections for the user" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  '%{amanda_user}'." >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
		PASSWD_OK=0
	else
		echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!                                                       !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  The '%{amanda_user}' user account for this system has been   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  created, however the user has no password set. For   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  security purposes this account  is normally locked   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  after creation.  Unfortunately,  when locking this   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  account an error occurred.  To ensure the security   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  of your system  you should set a password  for the   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  user account '%{amanda_user}' immediately!  To set  such a   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  password, please issue the following command.:       !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!                                                       !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!   # passwd %{amanda_user}                                   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!                                                       !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
		PASSWD_OK=1
	fi
else
	# log information about 'amanda' user parameters
	echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  The Amanda backup software is configured to operate as the" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  user '%{amanda_user}'.  This user exists on your system and has not" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  been modified.  To ensure that Amanda functions properly," >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  please see that the following parameters are set for that" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  user.:" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  SHELL:          /bin/sh" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  HOME:           %{LOCALSTATEDIR}" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  Default group:  %{amanda_group}" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  Verifying %{amanda_user} parameters :" >>${TMPFILE}

        if [ "`id -gn %{amanda_user}`" != "disk" ] ; then
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!!  user 'amandabackup' is not part of the disk group,Pl !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!!  make sure it is corrected before start using amanda  !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
        else
                echo "`date +'%b %e %Y %T'`:  Verified group name of user 'amandabackup'" >>${TMPFILE}
        fi

        if [ "`grep ^%{amanda_user} /etc/passwd|cut -d: -f7`" != "/bin/sh" ] ; then
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! user 'amandabackup' default shell should be set to    !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! /bin/sh, pl correct before start using amanda         !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
        else
                echo "`date +'%b %e %Y %T'`:  Verified Default shell for user 'amandabackup'" >>${TMPFILE}
        fi

        if [ "`grep ^%{amanda_user} /etc/passwd|cut -d: -f6`" != "%{LOCALSTATEDIR}" ] ; then
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! user 'amandabackup' home directory should be set to   !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! %{LOCALSTATEDIR} Pl correct before using amanda       !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
        else
                echo "`date +'%b %e %Y %T'`:  Verified Default home directory for user amandabackup" >>${TMPFILE}
        fi
	echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
	PASSWD_OK=0
fi
if [ -d %{LOCALSTATEDIR} ] ; then
	echo -n "`date +'%b %e %Y %T'`:  Checking ownership of '%{LOCALSTATEDIR}'... " >>${TMPFILE}
	if [ "`ls -dl %{LOCALSTATEDIR} | awk '//{split($_,x); print x[3]}'`" == "%{amanda_user}" ] && \
	   [ "`ls -dl %{LOCALSTATEDIR} | awk '//{split($_,x); print x[4]}'`" == "%{amanda_group}" ] ; then
		echo "correct." >>${TMPFILE}
		VARLIB_OK=0
	else
		echo "incorrect!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  Please ensure that the directory '%{LOCALSTATEDIR}' is owned by" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  the user '%{amanda_user}' and group '%{amanda_group}'." >>${TMPFILE}
		VARLIB_OK=1
	fi
else
	VARLIB_OK=0
fi
echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
if [ ! -e ${LOGDIR} ] ; then
	# create log directory
	mkdir -m 0750 ${LOGDIR} >>${TMPFILE} 2>&1
	chown %{amanda_user}:%{amanda_group} ${LOGDIR} >>${TMPFILE} 2>&1
elif [ ! -d ${LOGDIR} ] ; then
	mv ${LOGDIR} ${LOGDIR}.rpmsave >>${TMPFILE} 2>&1
	mkdir -m 0750 ${LOGDIR} >>${TMPFILE} 2>&1
	chown %{amanda_user}:%{amanda_group} ${LOGDIR} >>${TMPFILE} 2>&1
	mv ${LOGDIR}.rpmsave ${LOGDIR}/ >>${TMPFILE} 2>&1
fi

if [ ${PASSWD_OK} -eq 1 ] || [ ${VARLIB_OK} -eq 1 ] ; then
	cat ${TMPFILE}
	cat ${TMPFILE} >>${INSTALL_ERR}
	echo "Please review '${INSTALL_ERR}' to correct errors which have prevented the Amanda installaton." >&2
	echo "Amanda installation log can be found in '${INSTALL_LOG}' and errors (if any) in '${INSTALL_ERR}'."
	exit 1
else
	cat ${TMPFILE}
	cat ${TMPFILE} >>${INSTALL_LOG}
fi

echo "`date +'%b %e %Y %T'`: === Amanda installation started. ===" >${TMPFILE}

cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}
if [ -f "${TMPFILE}" ]; then
	rm -f "${TMPFILE}"
fi

%post
TMPFILE=`mktemp /tmp/rpm-amanda.XXXXXXXXXXX
if [ $? -ne 0 ]; then
	echo "Unable to mktemp!" 1>&2
	exit 1
fi
LOGDIR="%{LOGDIR}"
INSTALL_LOG="${LOGDIR}/install.log"
INSTALL_ERR="${LOGDIR}/install.err"

echo -n "`date +'%b %e %Y %T'`: Updating library cache..." >${TMPFILE}
/sbin/ldconfig >>${TMPFILE} 2>&1
echo "done." >>${TMPFILE}
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

echo "`date +'%b %e %Y %T'`: Installing '%{SYSCONFDIR}/amandates'." >${TMPFILE}
ret_val=0
if [ ! -f %{SYSCONFDIR}/amandates ] ; then
	touch %{SYSCONFDIR}/amandates >>${TMPFILE} 2>&1
	ret_val=$?
	if [ ${ret_val} -eq 0 ] ; then
		echo "`date +'%b %e %Y %T'`: The file '%{SYSCONFDIR}/amandates' has been created." >>${TMPFILE}
	fi
fi
if [ ${ret_val} -eq 0 ] ; then
	echo "`date +'%b %e %Y %T'`: Ensuring correct permissions for '%{SYSCONFDIR}/amandates'." >>${TMPFILE}
	chown %{amanda_user}:%{amanda_group} %{SYSCONFDIR}/amandates
	chmod 0640 %{SYSCONFDIR}/amandates >>${TMPFILE} 2>&1
fi
if [ ${ret_val} -eq 0 ]; then
	echo "`date +'%b %e %Y %T'`: '%{SYSCONFDIR}/amandates' Installation successful." >>${TMPFILE}
	cat ${TMPFILE}
	cat ${TMPFILE} >>${INSTALL_LOG}
else
	echo "`date +'%b %e %Y %T'`: '%{SYSCONFDIR}/amandates' Installation failed." >>${TMPFILE}
	cat ${TMPFILE}
	cat ${TMPFILE} >>${INSTALL_ERR}
fi

# Install .amandahosts
echo "`date +'%b %e %Y %T'`: Checking '%{LOCALSTATEDIR}/.amandahosts' file." >${TMPFILE}
if [ ! -f %{LOCALSTATEDIR}/.amandahosts ] ; then
	touch %{LOCALSTATEDIR}/.amandahosts >>${TMPFILE} 2>&1
fi
for host in localhost localhost.localdomain ; do
	if [ -z "`grep \"^${host}[[:blank:]]\+root[[:blank:]]\+amindexd[[:blank:]]\+amidxtaped\" %{LOCALSTATEDIR}/.amandahosts`" ] ; then
	 echo "${host}   root amindexd amidxtaped" >>%{LOCALSTATEDIR}/.amandahosts
	fi
	if [ -z "`grep \"^${host}[[:blank:]]\+%{amanda_user}[[:blank:]]\+amdump\" %{LOCALSTATEDIR}/.amandahosts`" ] ; then
	 echo "${host}   %{amanda_user} amdump" >>%{LOCALSTATEDIR}/.amandahosts
	fi
done
chown %{amanda_user}:%{amanda_group} %{LOCALSTATEDIR}/.amandahosts >>${TMPFILE} 2>&1
chmod 0600 %{LOCALSTATEDIR}/.amandahosts >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

# environment variables (~amandabackup/.profile)
echo "`date +'%b %e %Y %T'`: Checking for '%{LOCALSTATEDIR}/.profile' and ensuring correct environment." >${TMPFILE}
if [ ! -f %{LOCALSTATEDIR}/.profile ] ; then
	touch %{LOCALSTATEDIR}/.profile >>${TMPFILE} 2>&1
fi
if [ -z "`grep PATH %{LOCALSTATEDIR}/.profile | grep '%{SBINDIR}'`" ] ; then
	echo "export PATH=\"\$PATH:%{SBINDIR}\"" >>%{LOCALSTATEDIR}/.profile 2>>${TMPFILE}
fi
echo "`date +'%b %e %Y %T'`: Setting ownership and permissions for '%{LOCALSTATEDIR}/.profile'" >>${TMPFILE}
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}
chown %{amanda_user}:%{amanda_group} %{LOCALSTATEDIR}/.profile >>${TMPFILE} 2>&1
chmod 0640 %{LOCALSTATEDIR}/.profile >>${TMPFILE} 2>&1

echo "`date +'%b %e %Y %T'`: === Amanda installation complete. ===" >${TMPFILE}

cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

if [ -f "${TMPFILE}" ]; then
	rm -f "${TMPFILE}"
fi

echo "Amanda installation log can be found in '${INSTALL_LOG}' and errors (if any) in '${INSTALL_ERR}'."
%postun
/sbin/ldconfig
%pre backup_server
TMPFILE=`mktemp /tmp/rpm-amanda.XXXXXXXXXXX`
if [ $? -ne 0 ]; then
	echo "Unable to mktemp!" 1>&2
	exit 1
fi

LOGDIR="%{LOGDIR}"
INSTALL_LOG="${LOGDIR}/install.log"
INSTALL_ERR="${LOGDIR}/install.err"

echo "`date +'%b %e %Y %T'`: Preparing to install: %{amanda_version_info}" >${TMPFILE}

# Check for the 'amanda' user
echo "`date +'%b %e %Y %T'`: Checking for '%{amanda_user}' user..." >>${TMPFILE}
if [ "`id -u %{amanda_user} >&/dev/null && echo 0 || echo 1`" != "0" ] ; then
	useradd -c "Amanda" -M -g disk -d %{LOCALSTATEDIR} -s /bin/sh %{amanda_user}
	if [ %{is_suse10} -eq 1 ] || [ %{is_sles9} -eq 1 ] || [ %{is_sles10} -eq 1 ]; then
		PASSWD_EXIT=$?
	else
		# Lock the amanda account until admin sets password
		passwd -l %{amanda_user} >>/dev/null
		PASSWD_EXIT=$?
	fi
	if [ ${PASSWD_EXIT} -eq 0 ] ; then
		echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  The '%{amanda_user}; user account has been successfully created." >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  Furthermore, the account has been automatically locked for you" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  for security purposes.  Once a password for the  '%{amanda_user}'" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  account has been set, the user can be unlocked by issuing" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  the following command as root.:" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  # passwd -u %{amanda_user}" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  If this is not a new installation of Amanda and you have" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  pre-existing Amanda configurations in %{SYSCONFDIR}/amanda" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  you should ensure that 'dumpuser' is set to '%{amanda_user}'" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  in those configurations.  Additionally, you should ensure" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  that %{LOCALSTATEDIR}/.amandahosts on your client systems" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  is properly configured to allow connections for the user" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  '%{amanda_user}'." >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
		PASSWD_OK=0
	else
		echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!                                                       !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  The '%{amanda_user}' user account for this system has been   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  created, however the user has no password set. For   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  security purposes this account  is normally locked   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  after creation.  Unfortunately,  when locking this   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  account an error occurred.  To ensure the security   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  of your system  you should set a password  for the   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  user account '%{amanda_user}' immediately!  To set  such a   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  password, please issue the following command.:       !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!                                                       !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!   # passwd %{amanda_user}                                     !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!                                                       !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
		PASSWD_OK=1
	fi
else
	# log information about 'amanda' user parameters
	echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  The Amanda backup software is configured to operate as the" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  user '%{amanda_user}'.  This user exists on your system and has not" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  been modified.  To ensure that Amanda functions properly," >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  please see that the following parameters are set for that" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  user.:" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  SHELL:          /bin/sh" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  HOME:           %{LOCALSTATEDIR}" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  Default group:  %{amanda_group}" >>${TMPFILE}
        echo "`date +'%b %e %Y %T'`:  Verifying %{amanda_user} parameters :" >>${TMPFILE}

        if [ "`id -gn %{amanda_user}`" != "disk" ] ; then
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!!  user 'amandabackup' is not part of the disk group,Pl !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!!  make sure it is corrected before start using amanda  !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
        else
                echo "`date +'%b %e %Y %T'`:  Verified group name of user 'amandabackup'" >>${TMPFILE}
        fi

        if [ "`grep ^%{amanda_user} /etc/passwd|cut -d: -f7`" != "/bin/sh" ] ; then
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! user 'amandabackup' default shell should be set to    !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! /bin/sh, pl correct before start using amanda         !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
        else
                echo "`date +'%b %e %Y %T'`:  Verified Default shell for user 'amandabackup'" >>${TMPFILE}
        fi

        if [ "`grep ^%{amanda_user} /etc/passwd|cut -d: -f6`" != "%{LOCALSTATEDIR}" ] ; then
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! user 'amandabackup' home directory should be set to   !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! %{LOCALSTATEDIR} Pl correct before using amanda       !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
        else
                echo "`date +'%b %e %Y %T'`:  Verified Default home directory for user amandabackup" >>${TMPFILE}
        fi
	echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
	PASSWD_OK=0
fi
if [ -d %{LOCALSTATEDIR} ] ; then
	echo -n "`date +'%b %e %Y %T'`:  Checking ownership of '%{LOCALSTATEDIR}'... " >>${TMPFILE}
	if [ "`ls -dl %{LOCALSTATEDIR} | awk '//{split($_,x); print x[3]}'`" == "%{amanda_user}" ] && \
	   [ "`ls -dl %{LOCALSTATEDIR} | awk '//{split($_,x); print x[4]}'`" == "%{amanda_group}" ] ; then
		echo "correct." >>${TMPFILE}
		VARLIB_OK=0
	else
		echo "incorrect!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  Please ensure that the directory '%{LOCALSTATEDIR}' is owned by" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  the user '%{amanda_user}' and group '%{amanda_group}'." >>${TMPFILE}
		VARLIB_OK=1
	fi
else
	VARLIB_OK=0
fi
echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
if [ ! -e ${LOGDIR} ] ; then
	# create log directory
	mkdir -m 0750 ${LOGDIR} >>${TMPFILE} 2>&1
	chown %{amanda_user}:%{amanda_group} ${LOGDIR} >>${TMPFILE} 2>&1
elif [ ! -d ${LOGDIR} ] ; then
	mv ${LOGDIR} ${LOGDIR}.rpmsave >>${TMPFILE} 2>&1
	mkdir -m 0750 ${LOGDIR} >>${TMPFILE} 2>&1
	chown %{amanda_user}:%{amanda_group} ${LOGDIR} >>${TMPFILE} 2>&1
	mv ${LOGDIR}.rpmsave ${LOGDIR}/ >>${TMPFILE} 2>&1
fi
if [ ${PASSWD_OK} -eq 1 ] || [ ${VARLIB_OK} -eq 1 ] ; then
	cat ${TMPFILE}
	cat ${TMPFILE} >>${INSTALL_ERR}
	echo "Please review '${INSTALL_ERR}' to correct errors which have prevented the Amanda installaton." >&2
	echo "Amanda installation log can be found in '${INSTALL_LOG}' and errors (if any) in '${INSTALL_ERR}'."
	exit 1
else
	cat ${TMPFILE}
	cat ${TMPFILE} >>${INSTALL_LOG}
fi

echo "`date +'%b %e %Y %T'`: === Amanda backup server installation started. ===" >${TMPFILE}

cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}
if [ -f "${TMPFILE}" ]; then
	rm -f "${TMPFILE}"
fi
%post backup_server
TMPFILE=`mktemp /tmp/rpm-amanda.XXXXXXXXXXX`
if [ $? -ne 0 ]; then
	echo "Unable to mktemp!" 1>&2
	exit 1
fi
LOGDIR="%{LOGDIR}"
INSTALL_LOG="${LOGDIR}/install.log"
INSTALL_ERR="${LOGDIR}/install.err"
echo -n "`date +'%b %e %Y %T'`: Updating system library cache..." >${TMPFILE}
/sbin/ldconfig
echo "done." >>${TMPFILE}
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

echo "`date +'%b %e %Y %T'`: Installing '%{SYSCONFDIR}/amandates'." >${TMPFILE}
ret_val=0
if [ ! -f %{SYSCONFDIR}/amandates ] ; then
	touch %{SYSCONFDIR}/amandates >>${TMPFILE} 2>&1
	ret_val=$?
	if [ ${ret_val} -eq 0 ] ; then
		echo "`date +'%b %e %Y %T'`: The file '%{SYSCONFDIR}/amandates' has been created." >>${TMPFILE}
	fi
fi
if [ ${ret_val} -eq 0 ] ; then
	echo "`date +'%b %e %Y %T'`: Ensuring correct permissions for '%{SYSCONFDIR}/amandates'." >>${TMPFILE}
	chown %{amanda_user}:%{amanda_group} %{SYSCONFDIR}/amandates >>${TMPFILE} 2>&1
	chmod 0640 %{SYSCONFDIR}/amandates >>${TMPFILE} 2>&1
fi
if [ ${ret_val} -eq 0 ]; then
	echo "`date +'%b %e %Y %T'`: '%{SYSCONFDIR}/amandates' Installation successful." >>${TMPFILE}
	cat ${TMPFILE}
	cat ${TMPFILE} >>${INSTALL_LOG}
else
	echo "`date +'%b %e %Y %T'`: '%{SYSCONFDIR}/amandates' Installation failed." >>${TMPFILE}
	cat ${TMPFILE}
	cat ${TMPFILE} >>${INSTALL_ERR}
fi

# Install .amandahosts to server
echo "`date +'%b %e %Y %T'`: Checking '%{LOCALSTATEDIR}/.amandahosts' file." >${TMPFILE}
if [ ! -f %{LOCALSTATEDIR}/.amandahosts ] ; then
	touch %{LOCALSTATEDIR}/.amandahosts >>${TMPFILE} 2>&1
fi
for host in localhost localhost.localdomain ; do
	if [ -z "`grep \"^${host}[[:blank:]]\+root[[:blank:]]\+amindexd[[:blank:]]\+amidxtaped\" %{LOCALSTATEDIR}/.amandahosts`" ] ; then
	 echo "${host}   root amindexd amidxtaped" >>%{LOCALSTATEDIR}/.amandahosts
	fi
	if [ -z "`grep \"^${host}[[:blank:]]\+%{amanda_user}[[:blank:]]\+amdump\" %{LOCALSTATEDIR}/.amandahosts`" ] ; then
	 echo "${host}   %{amanda_user} amdump" >>%{LOCALSTATEDIR}/.amandahosts
	fi
done
chown %{amanda_user}:%{amanda_group} %{LOCALSTATEDIR}/.amandahosts >>${TMPFILE} 2>&1
chmod 0600 %{LOCALSTATEDIR}/.amandahosts >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

# environment variables (~amandabackup/.profile)
echo "`date +'%b %e %Y %T'`: Checking for '%{LOCALSTATEDIR}/.profile' and ensuring correct environment." >${TMPFILE}
if [ ! -f %{LOCALSTATEDIR}/.profile ] ; then
	touch %{LOCALSTATEDIR}/.profile >>${TMPFILE} 2>&1
fi
if [ -z "`grep PATH %{LOCALSTATEDIR}/.profile | grep '%{SBINDIR}'`" ] ; then
	echo "export PATH=\"\$PATH:%{SBINDIR}\"" >>%{LOCALSTATEDIR}/.profile 2>>${TMPFILE}
fi
echo "`date +'%b %e %Y %T'`: Setting ownership and permissions for '%{LOCALSTATEDIR}/.profile'" >>${TMPFILE}
chown %{amanda_user}:%{amanda_group} %{LOCALSTATEDIR}/.profile >>${TMPFILE} 2>&1
chmod 0640 %{LOCALSTATEDIR}/.profile >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

echo "`date +'%b %e %Y %T'`: === Amanda backup server installation complete. ===" >${TMPFILE}

cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}
if [ -f "${TMPFILE}" ]; then
	rm -f "${TMPFILE}"
fi

echo "Amanda installation log can be found in '${INSTALL_LOG}' and errors (if any) in '${INSTALL_ERR}'."
%postun backup_server
/sbin/ldconfig
%pre backup_client
TMPFILE=`mktemp /tmp/rpm-amanda.XXXXXXXXXXX`
if [ $? -ne 0 ]; then
	echo "Unable to mktemp!" 1>&2
	exit 1
fi
LOGDIR="%{LOGDIR}"
INSTALL_LOG="${LOGDIR}/install.log"
INSTALL_ERR="${LOGDIR}/install.err"

echo "`date +'%b %e %Y %T'`: Preparing to install: %{amanda_version_info}" >${TMPFILE}

# Check for the 'amanda' user
echo "`date +'%b %e %Y %T'`: Checking for '%{amanda_user}' user..." >>${TMPFILE}
if [ "`id -u %{amanda_user} >&/dev/null && echo 0 || echo 1`" != "0" ] ; then
	useradd -c "Amanda" -M -g disk -d %{LOCALSTATEDIR} -s /bin/sh %{amanda_user} >>${TMPFILE} 2>&1
	if [ %{is_suse10} -eq 1 ] || [ %{is_sles9} -eq 1 ] || [ %{is_sles10} -eq 1 ]; then
		PASSWD_EXIT=$?
	else
		# Lock the amanda account until admin sets password
		passwd -l %{amanda_user} >>/dev/null
		PASSWD_EXIT=$?
	fi
	if [ ${PASSWD_EXIT} -eq 0 ] ; then
		echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  The '%{amanda_user}; user account has been successfully created." >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  Furthermore, the account has been automatically locked for you" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  for security purposes.  Once a password for the  '%{amanda_user}'" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  account has been set, the user can be unlocked by issuing" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  the following command as root.:" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  # passwd -u %{amanda_user}" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  If this is not a new installation of Amanda and you have" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  pre-existing Amanda configurations in %{SYSCONFDIR}/amanda" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  you should ensure that 'dumpuser' is set to '%{amanda_user}'" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  in those configurations.  Additionally, you should ensure" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  that %{LOCALSTATEDIR}/.amandahosts on your client systems" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  is properly configured to allow connections for the user" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  '%{amanda_user}'." >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
		PASSWD_OK=0
	else
		echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!                                                       !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  The '%{amanda_user}' user account for this system has been   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  created, however the user has no password set. For   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  security purposes this account  is normally locked   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  after creation.  Unfortunately,  when locking this   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  account an error occurred.  To ensure the security   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  of your system  you should set a password  for the   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  user account '%{amanda_user}' immediately!  To set  such a   !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!  password, please issue the following command.:       !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!                                                       !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!   # passwd %{amanda_user}                                     !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!!                                                       !!!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
		PASSWD_OK=1
	fi
else
	# log information about 'amanda' user parameters
	echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  The Amanda backup software is configured to operate as the" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  user '%{amanda_user}'.  This user exists on your system and has not" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  been modified.  To ensure that Amanda functions properly," >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  please see that the following parameters are set for that" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  user.:" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  SHELL:          /bin/sh" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  HOME:           %{LOCALSTATEDIR}" >>${TMPFILE}
	echo "`date +'%b %e %Y %T'`:  Default group:  %{amanda_group}" >>${TMPFILE}
        echo "`date +'%b %e %Y %T'`:  Verifying %{amanda_user} parameters :" >>${TMPFILE}

        if [ "`id -gn %{amanda_user}`" != "disk" ] ; then
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! user 'amandabackup' is not part of the disk group,Pl  !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! make sure it is corrected before start using Amanda   !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
        else
                echo "`date +'%b %e %Y %T'`:  Verified group name of user 'amandabackup'" >>${TMPFILE}
        fi

        if [ "`grep ^%{amanda_user} /etc/passwd|cut -d: -f7`" != "/bin/sh" ] ; then
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! user 'amandabackup' default shell should be set to    !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! /bin/sh, pl correct before start using Amanda         !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
        else
                echo "`date +'%b %e %Y %T'`:  Verified Default shell for user 'amandabackup'" >>${TMPFILE}
        fi

        if [ "`grep ^%{amanda_user} /etc/passwd|cut -d: -f6`" != "%{LOCALSTATEDIR}" ] ; then
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! user 'amandabackup' home directory should be set to   !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! %{LOCALSTATEDIR} Pl correct before using Amanda       !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
        else
                echo "`date +'%b %e %Y %T'`:  Verified Default home directory for user amandabackup" >>${TMPFILE}
        fi

	echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
	PASSWD_OK=0
fi
if [ -d %{LOCALSTATEDIR} ] ; then
	echo -n "`date +'%b %e %Y %T'`:  Checking ownership of '%{LOCALSTATEDIR}'... " >>${TMPFILE}
	if [ "`ls -dl %{LOCALSTATEDIR} | awk '//{split($_,x); print x[3]}'`" == "%{amanda_user}" ] && \
	   [ "`ls -dl %{LOCALSTATEDIR} | awk '//{split($_,x); print x[4]}'`" == "%{amanda_group}" ] ; then
		echo "correct." >>${TMPFILE}
		VARLIB_OK=0
	else
		echo "incorrect!" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  Please ensure that the directory '%{LOCALSTATEDIR}' is owned by" >>${TMPFILE}
		echo "`date +'%b %e %Y %T'`:  the user '%{amanda_user}' and group '%{amanda_group}'." >>${TMPFILE}
		VARLIB_OK=1
	fi
else
	VARLIB_OK=0
fi
echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}

if [ ! -e ${LOGDIR} ] ; then
	# create log directory
	mkdir -m 0750 ${LOGDIR} >>${TMPFILE} 2>&1
	chown %{amanda_user}:%{amanda_group} ${LOGDIR} >>${TMPFILE} 2>&1
elif [ ! -d ${LOGDIR} ] ; then
	mv ${LOGDIR} ${LOGDIR}.rpmsave >>${TMPFILE} 2>&1
	mkdir -m 0750 ${LOGDIR} >>${TMPFILE} 2>&1
	chown %{amanda_user}:%{amanda_group} ${LOGDIR} >>${TMPFILE} 2>&1
	mv ${LOGDIR}.rpmsave ${LOGDIR}/ >>${TMPFILE} 2>&1
fi
if [ ${PASSWD_OK} -eq 1 ] || [ ${VARLIB_OK} -eq 1 ] ; then
	cat ${TMPFILE}
	cat ${TMPFILE} >>${INSTALL_ERR}
	echo "Please review '${INSTALL_ERR}' to correct errors which have prevented the Amanda installaton." >&2
	echo "Amanda installation log can be found in '${INSTALL_LOG}' and errors (if any) in '${INSTALL_ERR}'."
	exit 1
else
	cat ${TMPFILE}
	cat ${TMPFILE} >>${INSTALL_LOG}
fi

echo "`date +'%b %e %Y %T'`: === Amanda backup client installation started. ===" >${TMPFILE}

cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}
if [ -f "${TMPFILE}" ]; then
	rm -f "${TMPFILE}"
fi
%post backup_client
TMPFILE=`mktemp /tmp/rpm-amanda.XXXXXXXXXXX`
if [ $? -ne 0 ]; then
	echo "Unable to mktemp!" 1>&2
	exit 1
fi
LOGDIR="%{LOGDIR}"
INSTALL_LOG="${LOGDIR}/install.log"
INSTALL_ERR="${LOGDIR}/install.err"

echo -n "`date +'%b %e %Y %T'`: Updating system library cache..." >${TMPFILE}
/sbin/ldconfig
echo "done." >>${TMPFILE}
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

if [ ! -f %{SYSCONFDIR}/amandates ] ; then
	touch %{SYSCONFDIR}/amandates
	echo "`date +'%b %e %Y %T'`: The file '%{SYSCONFDIR}/amandates' has been created." >${TMPFILE}
else
	echo "`date +'%b %e %Y %T'`: Ensuring correct permissions for '%{SYSCONFDIR}/amandates'." >${TMPFILE}
fi
chown %{amanda_user}:%{amanda_group} %{SYSCONFDIR}/amandates >${TMPFILE} 2>&1
chmod 0640 %{SYSCONFDIR}/amandates >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

# Install .amandahosts to client
echo "`date +'%b %e %Y %T'`: Checking '%{LOCALSTATEDIR}/.amandahosts' file." >${TMPFILE}
if [ ! -f %{LOCALSTATEDIR}/.amandahosts ] ; then
	touch %{LOCALSTATEDIR}/.amandahosts >>${TMPFILE} 2>&1
fi
for host in localhost localhost.localdomain ; do
		if [ -z "`grep \"^${host}[[:blank:]]\+\" %{LOCALSTATEDIR}/.amandahosts | grep \"[[:blank:]]\+%{amanda_user}[[:blank:]]\+amdump\"`" ] ; then
			echo "${host}   %{amanda_user} amdump" >>%{LOCALSTATEDIR}/.amandahosts
		fi
done
chown %{amanda_user}:%{amanda_group} %{LOCALSTATEDIR}/.amandahosts >>${TMPFILE} 2>&1
chmod 0600 %{LOCALSTATEDIR}/.amandahosts >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

# environment variables (~amandabackup/.profile)
echo "`date +'%b %e %Y %T'`: Checking for '%{LOCALSTATEDIR}/.profile' and ensuring correct environment." >${TMPFILE}
if [ ! -f %{LOCALSTATEDIR}/.profile ] ; then
	touch %{LOCALSTATEDIR}/.profile >>${TMPFILE} 2>&1
fi
if [ -z "`grep PATH %{LOCALSTATEDIR}/.profile | grep '%{SBINDIR}'`" ] ; then
	echo "export PATH=\"\$PATH:%{SBINDIR}\"" >>%{LOCALSTATEDIR}/.profile 2>>${TMPFILE}
fi
echo "`date +'%b %e %Y %T'`: Setting ownership and permissions for '%{LOCALSTATEDIR}/.profile'" >>${TMPFILE}
chown %{amanda_user}:%{amanda_group} %{LOCALSTATEDIR}/.profile >>${TMPFILE} 2>&1
chmod 0640 %{LOCALSTATEDIR}/.profile >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

echo "`date +'%b %e %Y %T'`: === Amanda backup client installation complete. ===" >>${TMPFILE}

cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}
if [ -f "${TMPFILE}" ]; then
	rm -f "${TMPFILE}"
fi

echo "Amanda installation log can be found in '${INSTALL_LOG}' and errors (if any) in '${INSTALL_ERR}'."
%postun backup_client
/sbin/ldconfig

# --- Files to install ---

%files backup_client
%defattr(0755,%{amanda_user},%{amanda_group})
%dir %{LOCALSTATEDIR}
%dir %{LOCALSTATEDIR}/gnutar-lists
%dir %{LIBEXECDIR}
%{LIBDIR}/libamanda.a
%{LIBDIR}/libamanda.la
%{LIBDIR}/libamanda-%{version}.so
%{LIBDIR}/libamanda.so
%{LIBDIR}/libamandad.a
%{LIBDIR}/libamandad.la
%{LIBDIR}/libamandad-%{version}.so
%{LIBDIR}/libamandad.so
%{LIBDIR}/libamclient.a
%{LIBDIR}/libamclient.la
%{LIBDIR}/libamclient-%{version}.so
%{LIBDIR}/libamclient.so
%{LIBDIR}/librestore.a
%{LIBDIR}/librestore.la
%{LIBDIR}/librestore-%{version}.so
%{LIBDIR}/librestore.so
%{LIBEXECDIR}/amandad
%{LIBEXECDIR}/generic-dumper
%{LIBEXECDIR}/amgtar
%{LIBEXECDIR}/noop
%{LIBEXECDIR}/patch-system
%{LIBEXECDIR}/selfcheck
%{LIBEXECDIR}/sendbackup
%{LIBEXECDIR}/sendsize
%{LIBEXECDIR}/versionsuffix
%{SBINDIR}/amplot
%defattr(4750,root,disk)
%{LIBEXECDIR}/calcsize
%{LIBEXECDIR}/killpgrp
%{LIBEXECDIR}/rundump
%{LIBEXECDIR}/runtar
%defattr(0750,%{amanda_user},%{amanda_group})
%dir %{LOGDIR}
%{SBINDIR}/amaespipe
%{SBINDIR}/amcrypt
%{SBINDIR}/amcrypt-ossl
%{SBINDIR}/amcrypt-ossl-asym
%{SBINDIR}/amoldrecover
%{SBINDIR}/amrecover
%defattr(0644,%{amanda_user},%{amanda_group})
%{MANDIR}/man5/amanda.conf.5.gz
%{MANDIR}/man5/amanda-client.conf.5.gz
%{MANDIR}/man8/amanda.8.gz
%{MANDIR}/man8/amplot.8.gz
%{MANDIR}/man8/amrecover.8.gz
%{LOCALSTATEDIR}/example/amanda-client.conf
%{LIBEXECDIR}/amcat.awk
%{LIBEXECDIR}/amplot.awk
%{LIBEXECDIR}/amplot.g
%{LIBEXECDIR}/amplot.gp
%defattr(0640,%{amanda_user},%{amanda_group})
%{LOCALSTATEDIR}/amanda-release

%files backup_server
%defattr(0755,%{amanda_user},%{amanda_group})
%dir %{SYSCONFDIR}/amanda
%dir %{LIBEXECDIR}
%dir %{LOCALSTATEDIR}
%dir %{LOCALSTATEDIR}/gnutar-lists
%{LIBDIR}/libamanda.a
%{LIBDIR}/libamanda.la
%{LIBDIR}/libamanda-%{version}.so
%{LIBDIR}/libamanda.so
%{LIBDIR}/libamandad.a
%{LIBDIR}/libamandad.la
%{LIBDIR}/libamandad-%{version}.so
%{LIBDIR}/libamandad.so
%{LIBDIR}/libamclient.a
%{LIBDIR}/libamclient.la
%{LIBDIR}/libamclient-%{version}.so
%{LIBDIR}/libamclient.so
%{LIBDIR}/libamserver.a
%{LIBDIR}/libamserver.la
%{LIBDIR}/libamserver-%{version}.so
%{LIBDIR}/libamserver.so
%{LIBDIR}/libamtape.a
%{LIBDIR}/libamtape.la
%{LIBDIR}/libamtape-%{version}.so
%{LIBDIR}/libamtape.so
%{LIBDIR}/librestore.a
%{LIBDIR}/librestore.la
%{LIBDIR}/librestore-%{version}.so
%{LIBDIR}/librestore.so
%{LIBEXECDIR}/amandad
%{LIBEXECDIR}/amcleanupdisk
%{LIBEXECDIR}/amidxtaped
%{LIBEXECDIR}/amindexd
%{LIBEXECDIR}/amlogroll
%{LIBEXECDIR}/amtrmidx
%{LIBEXECDIR}/amtrmlog
%{LIBEXECDIR}/chg-lib.sh
%{LIBEXECDIR}/chg-chio
%{LIBEXECDIR}/chg-chs
%{LIBEXECDIR}/chg-disk
%{LIBEXECDIR}/chg-iomega
%{LIBEXECDIR}/chg-juke
%{LIBEXECDIR}/chg-manual
%{LIBEXECDIR}/chg-mcutil
%{LIBEXECDIR}/chg-mtx
%{LIBEXECDIR}/chg-multi
%{LIBEXECDIR}/chg-null
%{LIBEXECDIR}/chg-rait
%{LIBEXECDIR}/chg-rth
%{LIBEXECDIR}/chg-scsi
%{LIBEXECDIR}/chg-zd-mtx
%{LIBEXECDIR}/chunker
%{LIBEXECDIR}/driver
%{LIBEXECDIR}/generic-dumper
%{LIBEXECDIR}/amgtar
%{LIBEXECDIR}/noop
%{LIBEXECDIR}/patch-system
%{LIBEXECDIR}/selfcheck
%{LIBEXECDIR}/sendbackup
%{LIBEXECDIR}/sendsize
%{LIBEXECDIR}/taper
%{LIBEXECDIR}/versionsuffix
%{SBINDIR}/amadmin
%{SBINDIR}/amcheckdb
%{SBINDIR}/amcleanup
%{SBINDIR}/amdd
%{SBINDIR}/amdump
%{SBINDIR}/amfetchdump
%{SBINDIR}/amflush
%{SBINDIR}/amgetconf
%{SBINDIR}/amlabel
%{SBINDIR}/ammt
%{SBINDIR}/amoverview
%{SBINDIR}/amplot
%{SBINDIR}/amreport
%{SBINDIR}/amrestore
%{SBINDIR}/amrmtape
%{SBINDIR}/amstatus
%{SBINDIR}/amtape
%{SBINDIR}/amtapetype
%{SBINDIR}/amtoc
%{SBINDIR}/amverify
%{SBINDIR}/amverifyrun
%defattr(4750,root,disk)
%{LIBEXECDIR}/calcsize
%{LIBEXECDIR}/killpgrp
%{LIBEXECDIR}/rundump
%{LIBEXECDIR}/runtar
%{LIBEXECDIR}/dumper
%{LIBEXECDIR}/planner
%{SBINDIR}/amcheck
%defattr(0750,%{amanda_user},%{amanda_group})
%dir %{LOGDIR}
%dir %{LOCALSTATEDIR}/example
%{SBINDIR}/amaespipe
%{SBINDIR}/amcrypt
%{SBINDIR}/amcrypt-ossl
%{SBINDIR}/amcrypt-ossl-asym
%{SBINDIR}/amoldrecover
%{SBINDIR}/amrecover
%defattr(0644,%{amanda_user},%{amanda_group})
%{LIBEXECDIR}/amcat.awk
%{LIBEXECDIR}/amplot.awk
%{LIBEXECDIR}/amplot.g
%{LIBEXECDIR}/amplot.gp
%{MANDIR}/man5/amanda.conf.5.gz
%{MANDIR}/man5/amanda-client.conf.5.gz
%{MANDIR}/man8/amadmin.8.gz
%{MANDIR}/man8/amanda.8.gz
%{MANDIR}/man8/amcheck.8.gz
%{MANDIR}/man8/amcheckdb.8.gz
%{MANDIR}/man8/amcleanup.8.gz
%{MANDIR}/man8/amdd.8.gz
%{MANDIR}/man8/amdump.8.gz
%{MANDIR}/man8/amfetchdump.8.gz
%{MANDIR}/man8/amflush.8.gz
%{MANDIR}/man8/amgetconf.8.gz
%{MANDIR}/man8/amlabel.8.gz
%{MANDIR}/man8/ammt.8.gz
%{MANDIR}/man8/amoverview.8.gz
%{MANDIR}/man8/amplot.8.gz
%{MANDIR}/man8/amrecover.8.gz
%{MANDIR}/man8/amreport.8.gz
%{MANDIR}/man8/amrestore.8.gz
%{MANDIR}/man8/amrmtape.8.gz
%{MANDIR}/man8/amstatus.8.gz
%{MANDIR}/man8/amtape.8.gz
%{MANDIR}/man8/amtapetype.8.gz
%{MANDIR}/man8/amtoc.8.gz
%{MANDIR}/man8/amverify.8.gz
%{MANDIR}/man8/amverifyrun.8.gz
%{MANDIR}/man8/amcrypt.8.gz
%{MANDIR}/man8/amcrypt-ossl.8.gz
%{MANDIR}/man8/amcrypt-ossl-asym.8.gz
%{MANDIR}/man8/amaespipe.8.gz
%{LOCALSTATEDIR}/example/amanda.conf
%{LOCALSTATEDIR}/example/amanda-client.conf
%{LOCALSTATEDIR}/example/label-templates/DLT.ps
%{LOCALSTATEDIR}/example/label-templates/EXB-8500.ps
%{LOCALSTATEDIR}/example/label-templates/HP-DAT.ps
%{LOCALSTATEDIR}/example/label-templates/8.5x11.ps
%{LOCALSTATEDIR}/example/label-templates/3hole.ps
%{LOCALSTATEDIR}/example/label-templates/DIN-A4.ps
%defattr(0640,%{amanda_user},%{amanda_group})
%{LOCALSTATEDIR}/amanda-release

# --- ChangeLog

%changelog
* Fri Sep 21 2007 Paddy Sreenivasan <paddy at zmanda dot com>
- Remove libamserver, libamtape from client rpm
* Wed Sep 19 2007 Paddy Sreenivasan <paddy at zmanda dot com>
- Added Fedora 7
* Tue Jun 26 2007 Kevin Till <ktill at zmanda dot com>
- set debug log to /var/log/amanda
* Fri Jan 12 2007 Paddy Sreenivasan <paddy at zmanda dot com>
- Added label templates
* Thu Dec 07 2006 Paddy Sreenivasan <paddy at zmanda dot com>
- Application API changes
* Fri Jun 16 2006 Kevin Till <ktill at zmanda dot com>
- make install will install necessary example files. 
  No need to "cp"
* Wed Jun 07 2006 Paddy Sreenivasan <paddy at zmanda dot com> -
- Added amoldrecover and amanda-client.conf man page.
* Thu Jun 01 2006 Kevin Till <ktill at zmanda dot com> -
- Added amcrypt-ossl, amcrypt-ossl-asym by Ben Slusky.
* Thu May 18 2006 Paddy Sreenivasan <paddy at zmanda dot com> -
- Added SLES10, RHEL3 build options.
* Tue May 09 2006 Chris Lee <cmlee at zmanda dot com> -
- Added amanda-release file to amandabackup home directory.
- Installation message logging cleanup.
* Thu Apr 27 2006 Paddy Sreenivasan <paddy at zmanda dot com> -
- Removed dependency on tar version.
- Moved log directory creation after backup user creation.
* Wed Apr 19 2006 Chris Lee <cmlee at zmanda dot com> -
- Added informative message to note the location of pre- and post-
- install script logs files.
* Mon Apr 17 2006 Chris Lee <cmlee at zmanda dot com> -
- Reworked installation message logging and reporting.
* Fri Apr 14 2006 Chris Lee <cmlee at zmanda dot com> -
- Changed behavior for creating required localhost entries in the
- amandahosts file to check for these entries even when the file
- already exists.
* Wed Apr 12 2006 Chris Lee <cmlee at zmanda dot com> -
- Removed pre-install check for "disk" group.  This group should exist
- by default on almost all modern distributions.
* Tue Apr 11 2006 Chris Lee <cmlee at zmanda dot com> -
- Added amandahosts entry for "localhost" without domain.
* Fri Apr 07 2006 Chris Lee <cmlee at zmanda dot com> -
- Changed default entries in .amandahosts to use "localdomain" instead
- of "localnet".
- Updated amanda_version and release.
* Mon Apr 03 2006 Chris Lee <cmlee at zmanda dot com> -
- Added example amanda.conf to files.
* Thu Mar 16 2006 Chris Lee <cmlee at zmanda dot com> -
- Corrected an issue with pre-install scripts wrt bug #218.
- Corrected an issue with post-install scripts and added testing .profile 
- in amandabackup's home directory for setting environment variables wrt
- bug #220.
* Mon Mar 13 2006 Chris Lee <cmlee at zmanda dot com> -
- Corrected a syntactical error with setting ownership of amandates file
- wrt bug #216.
* Wed Mar 08 2006 Chris Lee <cmlee at zmanda dot com> -
- Added pre-install scripts to verify proper ownership of
- amandabackup home directory.
* Thu Feb 2 2006 Paddy Sreenivasan <paddy at zmanda dot com> -
- Require xinetd. Require termcap and initscripts for Fedora and Redhat.
* Mon Jan 09 2006 Chris Lee <cmlee at zmanda dot com> -
- Pre/post install scripts updated:
- o Resolved an issue where an empty amandates file was installed
-   even if the file already existed on the system.
- o If .amandahosts does not exist a default is now created.
- The Amanda user account has been changed to 'amandabackup' for
- additional security.
* Tue Jan 03 2006 Paddy Sreenivasan <paddy at zmanda dot com> -
- Removed amandates from files list.
* Thu Dec 29 2005 Chris Lee <cmlee at zmanda dot com> -
- Corrected dependency for awk to "/bin/awk".
* Thu Dec 29 2005 Kevin Till <ktill at zmanda dot com> -
- add man pages for amcrypt and amaespipe
* Thu Dec 29 2005 Chris Lee <cmlee at zmanda dot com> -
- Updated dependancy info to depend on tar >= 1.15.
- Included dependancies from top-level package in backup_client and
- backup_server packages.
- Reorganized files lists for readability (alphabetically).
- Updated backup_client files list to include some missing files per
- bug #129.
- Updated pre- and post-install to handle potential issue when
- /var/log/amanda exists and is a file rather than a directory.
- Corrected permissions for /var/log/amanda in pre-install scripts
- per bug #78 and 13 December change.
* Thu Dec 22 2005 Paddy Sreenivasan <paddy at zmanda dot com> -
- Added amaespipe and amcrypt
- Added sles9 build definitions
* Tue Dec 13 2005 Chris Lee <cmlee at zmanda dot com> -
- Changed permissions for /var/log/amanda, removing set group id bit.
- Added /etc/amandates to backup_client package.
* Thu Dec 08 2005 Chris Lee <cmlee at zmanda dot com> -
- Corrected an issue with detection of existing 'amanda' user account.
- Corrected ownership of setuid executables per Bug #66.
- Moved the gnutar and noop files to the backup_client package (where
- they sould be).
- Removed amqde from files list.
- Added logging features to pre- and post-install scripts.
* Wed Dec 07 2005 Chris Lee <cmlee at zmanda dot com> -
- Changed a number of directory and file permissions from amanda:root
- to amanda:disk in response to Bug #57.
* Fri Dec 02 2005 Chris Lee <cmlee at zmanda dot com> -
- Corrected SYSCONFDIR path definition.  Closes Bug #58.
* Mon Nov 28 2005 Chris Lee <cmlee at zmanda dot com> -
- Really fixed user creation preinstall scripts.
* Wed Nov 23 2005 Paddy Sreenivasan <paddy at zmanda dot com> -
- Updated package description.
- Changed Group for packages.
* Tue Nov 22 2005 Chris Lee <cmlee at zmanda dot com> -
- Corrected dependancy packaging issue with amanda libraries.
- Fixed creation of amanda user on systems which it does not exist.
- Corrected Group definition for SuSE.
- Updated descriptions to include amanda-libs package.
- Updated release number to 2.
* Tue Nov 08 2005 Chris Lee <cmlee at zmanda dot com> -
- Permissions changes: now using user=amanda, group=disk
* Sun Oct 30 2005 Chris Lee <cmlee at zmanda dot com> -
- Parameters to configure options --with-user and --with-group changed
- such that when test_build is set to '1' the username of the user who
- runs rpmbuild is used for both values.  If test_build is set to '0'
- then root is used for both values.
- The release field was also changed to automatically reflect the
- distribution and distribution release version for which the RPM was
- built.
* Tue Oct 18 2005 Chris Lee <cmlee at zmanda dot com> - 
- Initial RPM SPEC file created.

