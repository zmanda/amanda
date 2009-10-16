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
#  Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
#  Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
#


%define build_srpm 0
%{?srpm_only: %define build_srpm 1}

# Pkg-config sometimes needs its own path set, and we need to allow users to
# override our guess during detection.  This macro takes care of that.
# If no --define PKG_CONFIG_PATH was passed and env var $PKG_CONFIG_PATH is 
# set then use the env var.
%{!?PKG_CONFIG_PATH: %{expand:%(echo ${PKG_CONFIG_PATH:+"%%define PKG_CONFIG_PATH $PKG_CONFIG_PATH"})}}

%{?PKG_CONFIG_PATH:%{echo:PKG_CONFIG_PATH = %{PKG_CONFIG_PATH}}}

# Define which Distribution we are building:
# Try to detect the distribution we are building:
%if %{_vendor} == redhat 
    # Fedora symlinks /etc/fedora-release to /etc/redhat-release for at least
    # fc3-8.  So RHEL and Fedora look at the same file.  Different versions have
    # different numbers of spaces; hence the use if $3 vs. $4..
    %if %(awk '$1 == "Fedora" && $4 ~ /3.*/ { exit 1; }' /etc/redhat-release; echo $?)
        %define dist fedora
        %define disttag fc
        %define distver 3
	%define requires_libtermcap Requires: libtermcap.so.2
    %endif
    %if %(awk '$1 == "Fedora" && $4 ~ /4.*/ { exit 1; }' /etc/redhat-release; echo $?)
        %define dist fedora
        %define disttag fc
        %define distver 4
	%define requires_libtermcap Requires: libtermcap.so.2
    %endif
    %if %(awk '$1 == "Fedora" && $4 ~ /5.*/ { exit 1; }' /etc/redhat-release; echo $?)
        %define dist fedora
        %define disttag fc
        %define distver 5
	%define requires_libtermcap Requires: libtermcap.so.2
    %endif
    %if %(awk '$1 == "Fedora" && $4 ~ /6.*/ { exit 1; }' /etc/redhat-release; echo $?)
        %define dist fedora
        %define disttag fc
        %define distver 6
	%define requires_libtermcap Requires: libtermcap.so.2
    %endif
    %if %(awk '$1 == "Fedora" && $3 ~ /7.*/ { exit 1; }' /etc/redhat-release; echo $?)
        %define dist fedora
        %define disttag fc
        %define distver 7
	%define requires_libtermcap Requires: libtermcap.so.2
    %endif
    # if macro cannot have an empty test and we're just testing the existance
    %if %{?fedora:yes}%{!?fedora:no} == yes
        %define dist fedora
        %define disttag fc
        %define distver %{fedora}
	%if %{distver} <= 8
	    %define requires_libtermcap Requires: libtermcap.so.2
	%endif
        %if %{_host_cpu} == x86_64 && %{_target_cpu} == i686
                # Do nothing if PKG_CONFIG_PATH was set by the user above.
                %{!?PKG_CONFIG_PATH: %define PKG_CONFIG_PATH /usr/lib/pkgconfig}
        %endif
    %endif
    %if %(awk '$1 == "Red" && $7 ~ /3.*/ { exit 1; }' /etc/redhat-release; echo $?)
        %define dist redhat
        %define disttag rhel
        %define distver 3
        %define tarver 1.14
	%define requires_libtermcap Requires: libtermcap.so.2
	%define without_ipv6 --without-ipv6
    %endif
    %if %(awk '$1 == "Red" && $7 ~ /4.*/ { exit 1; }' /etc/redhat-release; echo $?)
        %define dist redhat
        %define disttag rhel
        %define distver 4
        %define tarver 1.14
	%define requires_libtermcap Requires: libtermcap.so.2
    %endif
    %if %(awk '$1 == "CentOS" && $3 ~ /4.*/ { exit 1; }' /etc/redhat-release; echo $?)
	%define dist redhat
	%define disttag rhel
	%define distver 4
	%define tarver 1.14
	%define requires_libtermcap Requires: libtermcap.so.2
    %endif
    %if %(awk '$1 == "Red" && $7 ~ /5.*/ { exit 1; }' /etc/redhat-release; echo $?)
        %define dist redhat
        %define disttag rhel
        %define distver 5
	%define requires_libtermcap Requires: libtermcap.so.2
    %endif
    %if %(awk '$1 == "CentOS" && $3 ~ /5.*/ { exit 1; }' /etc/redhat-release; echo $?)
        %define dist redhat
        %define disttag rhel
        %define distver 5
	%define requires_libtermcap Requires: libtermcap.so.2
    %endif
    
    # If dist is undefined, we didn't detect.
    %{!?dist:%define dist unknown}
%endif
# Detect Suse variants. 
%if %{_vendor} == "suse"
    %define dist SuSE
    %define disttag %(awk '$1=="SUSE" {$3=="Enterprise" ? TAG="sles" : TAG="suse" ; print TAG}' /etc/SuSE-release)
    %define distver %(awk '$1=="SUSE" {$3=="Enterprise" ? VER=$5 : VER=$3 ; print VER}' /etc/SuSE-release)
%endif

# Set options per distribution
%if %{dist} == redhat || %{dist} == fedora
    %define rpm_group Applications/Archiving
    %define xinetd_reload restart
    %define requires_initscripts Requires: initscripts
%endif
%if %{dist} == SuSE
    %define rpm_group Productivity/Archiving/Backup
    %define xinetd_reload restart
%endif

# Let's die if we haven't detected the distro. This might save some frustration.
# RPM does not provide a way to  exit gracefully, hence the tag_to_cause_exit. 
%{!?distver: %{error:"Your distribution and its version were not detected."}; %tag_to_cause_exit }
# Set minimum tar version if it wasn't set in the per-distro section
%{!?tarver: %define tarver 1.15}

%define packer %(%{__id_u} -n)

# --- Definitions ---

# Define amanda_version if it is not already defined.
%{!?amanda_version: %define amanda_version 2.6.2alpha}
%{!?amanda_release: %define amanda_release 1}
%define amanda_version_info "Amanda Community Edition - version %{amanda_version}"
%define amanda_user amandabackup
%define amanda_group disk
%define udpportrange "700,740"
%define tcpportrange "11000,11040"
%define low_tcpportrange "700,710"

Summary: The Amanda Backup and Archiving System
Name: amanda
Version: %{amanda_version}
%define rpm_release %{amanda_release}.%{disttag}%{distver}
%if %{build_srpm}
%define rpm_release %{amanda_release}
%endif
Release: %{rpm_release}
Source: %{name}-%{version}.tar.gz
License: http://wiki.zmanda.com/index.php/Amanda_Copyright
Vendor: Zmanda, Inc.
Packager: www.zmanda.com
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-%{packer}-buildroot
Group: %{rpm_group}
# TODO - Need required versions for these:
BuildRequires: autoconf
BuildRequires: automake
BuildRequires: binutils
BuildRequires: bison
BuildRequires: flex
BuildRequires: gcc
BuildRequires: glibc >= 2.2.0
BuildRequires: readline
# Note: newer distros have changed most *-devel to lib*-devel, and added a
# provides tag for backwards compat.
BuildRequires: readline-devel
BuildRequires: curl >= 7.10.0
BuildRequires: curl-devel >= 7.10.0
BuildRequires: openssl
BuildRequires: openssl-devel
BuildRequires: perl(ExtUtils::Embed)
Requires: /bin/awk
Requires: /bin/date
Requires: /usr/bin/id
Requires: /sbin/ldconfig
Requires: /bin/sh
Requires: /usr/sbin/useradd
Requires: /usr/sbin/usermod
Requires: fileutils
Requires: grep
Requires: gnuplot
Requires: libc.so.6
Requires: libm.so.6
Requires: libnsl.so.1
Requires: curl >= 7.10.0
Requires: openssl
Requires: xinetd
Requires: perl >= 5.6.0
Requires: tar >= %{tarver}
Requires: readline
%{?requires_libtermcap}
%{?requires_initscripts}
Provides: amanda-backup_client = %{amanda_version}, amanda-backup_server = %{amanda_version}

%package backup_client
Summary: The Amanda Backup and Archiving Client
Group: %{rpm_group}
Requires: /bin/awk
Requires: fileutils
Requires: grep
%{?requires_libtermcap}
%{?requires_initscripts}
Requires: xinetd
Requires: libc.so.6
Requires: libm.so.6
Requires: libnsl.so.1
Requires: perl >= 5.6.0
Requires: tar >= %{tarver}
Requires: readline
Provides: amanda-backup_client = %{amanda_version}
Provides: libamclient-%{version}.so = %{amanda_version}
Provides: libamanda-%{version}.so = %{amanda_version}
Conflicts: amanda-backup_server
# Native package names
Obsoletes: amanda, amanda-client, amanda-server

%package backup_server
Summary: The Amanda Backup and Archiving Server
Group: %{rpm_group}
Requires: /bin/awk
Requires: fileutils
Requires: grep
Requires: libc.so.6
Requires: libm.so.6
Requires: libnsl.so.1
%{?requires_libtermcap}
%{?requires_initscripts}
Requires: xinetd
Requires: perl >= 5.6.0
Requires: tar >= %{tarver}
Provides: amanda-backup_server = %{amanda_version}
Provides: amanda-backup_client = %{amanda_version}
Provides: libamclient-%{version}.so = %{amanda_version}
Provides: libamanda-%{version}.so = %{amanda_version}
Provides: libamserver-%{version}.so = %{amanda_version}
Provides: librestore-%{version}.so = %{amanda_version}
Provides: libamtape-%{version}.so = %{amanda_version}
Provides: libamdevice-%{version}.so = %{amanda_version}
Conflicts: amanda-backup_client
# Native package names
Obsoletes: amanda, amanda-client, amanda-server
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
%define PREFIX          /usr
%define EPREFIX         %{PREFIX}
%define BINDIR          %{EPREFIX}/bin
%define SBINDIR         %{EPREFIX}/sbin
%define LIBEXECDIR      %{EPREFIX}/libexec
%define AMLIBEXECDIR    %{LIBEXECDIR}/amanda
%define DATADIR         %{PREFIX}/share
%define SYSCONFDIR      /etc
%define LOCALSTATEDIR   /var
%define AMANDATES       %{AMANDAHOMEDIR}/amandates
%define AMANDAHOMEDIR   %{LOCALSTATEDIR}/lib/amanda
%ifarch x86_64
%define LIBDIR          %{EPREFIX}/lib64
%else
%define LIBDIR          %{EPREFIX}/lib
%endif
%define AMLIBDIR        %{LIBDIR}/amanda
%define INCLUDEDIR      %{PREFIX}/include
%define MANDIR          %{DATADIR}/man
%define LOGDIR          /var/log/amanda
%define PERLSITELIB     %(eval "`perl -V:installsitelib`"; echo $installsitelib)
%define AMDATADIR	/var/lib/amanda

# Installation directories:
%define ROOT_SBINDIR            %{buildroot}/%{SBINDIR}
%define ROOT_LIBEXECDIR         %{buildroot}/%{LIBEXECDIR}
%define ROOT_DATADIR            %{buildroot}/%{DATADIR}
%define ROOT_LOCALSTATEDIR      %{buildroot}/%{LOCALSTATEDIR}
%define ROOT_SYSCONFDIR         %{buildroot}/%{SYSCONFDIR}
%define ROOT_AMANDAHOMEDIR      %{buildroot}/%{AMANDAHOMEDIR}
%define ROOT_LIBDIR             %{buildroot}/%{LIBDIR}
%define ROOT_MANDIR             %{buildroot}/%{MANDIR}
%define ROOT_LOGDIR             %{buildroot}/%{LOGDIR}
%define ROOT_AMDATADIR          %{buildroot}/%{AMDATADIR}

# --- Unpack ---

%prep
%setup -q
# --- Configure and compile ---

%build
%define config_user %{amanda_user}
%define config_group %{amanda_group}

# Set PKG_CONFIG_PATH=some/path if some/path was set on the command line, or by 
# the platform detection bits.
# without_ipv6 should only be defined on rhel3.
./configure \
        %{?PKG_CONFIG_PATH: PKG_CONFIG_PATH=%PKG_CONFIG_PATH} \
        CFLAGS="%{optflags} -g -pipe" CXXFLAGS="%{optflags}" \
        --quiet \
        --prefix=%{PREFIX} \
        --sysconfdir=%{SYSCONFDIR} \
        --sharedstatedir=%{LOCALSTATEDIR} \
        --localstatedir=%{LOCALSTATEDIR} \
        --libdir=%{LIBDIR} \
        --includedir=%{INCLUDEDIR} \
	--with-amdatadir=%{AMDATADIR} \
        --with-gnuplot=/usr/bin/gnuplot \
        --with-gnutar=/bin/tar \
        --with-gnutar-listdir=%{AMANDAHOMEDIR}/gnutar-lists \
        --with-index-server=localhost \
        --with-tape-server=localhost \
        --with-user=%{config_user} \
        --with-group=%{config_group} \
        --with-owner=%{packer} \
        --with-fqdn \
        --with-bsd-security \
        --with-bsdtcp-security \
        --with-bsdudp-security \
        --with-ssh-security \
        --with-udpportrange=%{udpportrange} \
        --with-tcpportrange=%{tcpportrange} \
        --with-low-tcpportrange=%{low_tcpportrange} \
        --with-debugging=%{LOGDIR} \
        --with-assertions \
        --disable-installperms \
        %{?without_ipv6}

make -s LIBTOOLFLAGS=--silent

# --- Install to buildroot ---

%install
if [ "%{buildroot}" != "/" ]; then
        if [ -d "%{buildroot}" ] ; then
                rm -rf %{buildroot}
        fi
else
        echo "BuildRoot was somehow set to / !"
        exit -1
fi

make -s -j1 LIBTOOLFLAGS=--silent DESTDIR=%{buildroot} install

rm -f %{ROOT_AMANDAHOMEDIR}/example/inetd.conf.amandaclient
mkdir %{buildroot}/{etc,var/log}
mkdir %{ROOT_LOCALSTATEDIR}/amanda 
mkdir %{ROOT_SYSCONFDIR}/amanda
mkdir %{ROOT_AMANDAHOMEDIR}/gnutar-lists
mkdir %{ROOT_LOGDIR}

echo "%{amanda_version_info}" >%{ROOT_AMANDAHOMEDIR}/amanda-release

# --- Clean up buildroot ---

%clean
if [ "%{buildroot}" != "/" ]; then
        if [ -d "%{buildroot}" ] ; then
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
if [ "`id -u %{amanda_user} > /dev/null 2>&1 && echo 0 || echo 1`" != "0" ] ; then
        useradd -c "Amanda" -M -g %{amanda_group} -d %{AMANDAHOMEDIR} -s /bin/sh %{amanda_user}
        if [ %{dist} = "SuSE" ]; then
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
                echo "`date +'%b %e %Y %T'`:  that %{AMANDAHOMEDIR}/.amandahosts on your client systems" >>${TMPFILE}
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
        echo "`date +'%b %e %Y %T'`:  HOME:           %{AMANDAHOMEDIR}" >>${TMPFILE}
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
                echo "`date +'%b %e %Y %T'`:  !!! /bin/sh, pl correct before start using Amanda         !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
        else
                echo "`date +'%b %e %Y %T'`:  Verified Default shell for user 'amandabackup'" >>${TMPFILE}
        fi

        if [ "`grep ^%{amanda_user} /etc/passwd|cut -d: -f6`" != "%{AMANDAHOMEDIR}" ] ; then
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! user 'amandabackup' home directory should be set to   !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! %{AMANDAHOMEDIR} Pl correct before using Amanda       !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
        else
                echo "`date +'%b %e %Y %T'`:  Verified Default home directory for user amandabackup" >>${TMPFILE}
        fi
        echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
        PASSWD_OK=0
fi
if [ -d %{AMANDAHOMEDIR} ] ; then
        echo -n "`date +'%b %e %Y %T'`:  Checking ownership of '%{AMANDAHOMEDIR}'... " >>${TMPFILE}
        if [ "`ls -dl %{AMANDAHOMEDIR} | awk '//{split($_,x); print x[3]}'`" = "%{amanda_user}" ] && \
           [ "`ls -dl %{AMANDAHOMEDIR} | awk '//{split($_,x); print x[4]}'`" = "%{amanda_group}" ] ; then
                echo "correct." >>${TMPFILE}
                VARLIB_OK=0
        else
                echo "incorrect!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  Please ensure that the directory '%{AMANDAHOMEDIR}' is owned by" >>${TMPFILE}
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

if [ -e /etc/xinetd.d ] && [ -d /etc/xinetd.d ] ; then
        if [ ! -f /etc/xinetd.d/amandaserver ] ; then
                cp %{AMANDAHOMEDIR}/example/xinetd.amandaserver /etc/xinetd.d/amandaserver
                chmod 0644 /etc/xinetd.d/amandaserver >>${TMPFILE} 2>&1
                if [ -f /etc/xinetd.d/amandaclient ] ; then
                        rm /etc/xinetd.d/amandaclient
                fi
                echo -n "`date +'%b %e %Y %T'`: Reloading xinetd configuration..." >${TMPFILE}
                if [ "%{xinetd_reload}" == "reload" ] ; then
                        /etc/init.d/xinetd %{xinetd_reload} >>${TMPFILE} 2>&1
                        ret_val=$?
                        if [ ${ret_val} -ne 0 ] ; then
                                echo -n "reload failed.  Attempting restart..." >>${TMPFILE}
                                /etc/init.d/xinetd restart >>${TMPFILE} 2>&1
                                ret_val=$?
                        fi
                else
                        /etc/init.d/xinetd %{xinetd_reload} >>${TMPFILE} 2>&1
                        ret_val=$?
                fi
                if [ ${ret_val} -eq 0 ] ; then
                        echo "success." >>${TMPFILE}
                        cat ${TMPFILE}
                        cat ${TMPFILE} >>${INSTALL_LOG}
                else
                        echo "failed.  Please check your system logs." >>${TMPFILE}
                        cat ${TMPFILE} 1>&2
                        cat ${TMPFILE} >>${INSTALL_ERR}
                fi
        fi
fi

echo "`date +'%b %e %Y %T'`: Installing '%{AMANDATES}'." >${TMPFILE}
ret_val=0
if [ ! -f %{AMANDATES} ] ; then
        touch %{AMANDATES} >>${TMPFILE} 2>&1
        ret_val=$?
        if [ ${ret_val} -eq 0 ]; then
                echo "`date +'%b %e %Y %T'`: The file '%{AMANDATES}' has been created." >>${TMPFILE}
        fi
fi
if [ ${ret_val} -eq 0 ]; then
        echo "`date +'%b %e %Y %T'`: Ensuring correct permissions for '%{AMANDATES}'." >>${TMPFILE}
        chown %{amanda_user}:%{amanda_group} %{AMANDATES} >>${TMPFILE} 2>&1
        chmod 0640 %{AMANDATES} >>${TMPFILE} 2>&1
        if [ -x /sbin/restorecon ] ; then
              /sbin/restorecon %{AMANDATES}  >>${TMPFILE} 2>&1
        fi
fi
if [ ${ret_val} -eq 0 ]; then
        echo "`date +'%b %e %Y %T'`: '%{AMANDATES}' Installation successful." >>${TMPFILE}
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_LOG}
else
        echo "`date +'%b %e %Y %T'`: '%{AMANDATES}' Installation failed." >>${TMPFILE}
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_ERR}
fi


# Install .gnupg directory
echo "`date +'%b %e %Y %T'`: Installing '%{AMANDAHOMEDIR}/.gnupg'." >${TMPFILE}
ret_val=0
if [ ! -d %{AMANDAHOMEDIR}/.gnupg ] ; then
        echo "`date +'%b %e %Y %T'`: '%{AMANDAHOMEDIR}/.gnupg' will be created." >>${TMPFILE}
        mkdir %{AMANDAHOMEDIR}/.gnupg >>${TMPFILE} 2>&1
        ret_val=$?
        if [ ${ret_val} -eq 0 ]; then
                echo "`date +'%b %e %Y %T'`: The directory '%{AMANDAHOMEDIR}/.gnupg' created successfully." >>${TMPFILE}
        else
                echo "`date +'%b %e %Y %T'`: The directory '%{AMANDAHOMEDIR}/.gnupg' creation failed." >>${TMPFILE}
        fi
fi
if [ ${ret_val} -eq 0 ]; then
        echo "`date +'%b %e %Y %T'`: Ensuring correct permissions for '%{AMANDAHOMEDIR}/.gnupg'." >>${TMPFILE}
        chown %{amanda_user}:%{amanda_group} %{AMANDAHOMEDIR}/.gnupg >>${TMPFILE} 2>&1
        ret_val=$?
        if [ ${ret_val} -eq 0 ]; then
                chmod 700 %{AMANDAHOMEDIR}/.gnupg >>${TMPFILE} 2>&1
                ret_val=$?
        fi
fi
if [ ${ret_val} -eq 0 ]; then
        echo "`date +'%b %e %Y %T'`: '%{AMANDAHOMEDIR}/.gnupg' Installation successful." >>${TMPFILE}
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_LOG}
else
        echo "`date +'%b %e %Y %T'`: '%{AMANDAHOMEDIR}/.gnupg' Installation failed." >>${TMPFILE}
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_ERR}
fi

# Install .amandahosts
echo "`date +'%b %e %Y %T'`: Checking '%{AMANDAHOMEDIR}/.amandahosts' file." >${TMPFILE}
if [ ! -f %{AMANDAHOMEDIR}/.amandahosts ] ; then
        touch %{AMANDAHOMEDIR}/.amandahosts >>${TMPFILE} 2>&1
fi
for host in localhost localhost.localdomain ; do
        if [ -z "`grep \"^${host}[[:blank:]]\+root[[:blank:]]\+amindexd[[:blank:]]\+amidxtaped\" %{AMANDAHOMEDIR}/.amandahosts`" ] ; then
                echo "${host}   root amindexd amidxtaped" >>%{AMANDAHOMEDIR}/.amandahosts
        fi
        if [ -z "`grep \"^${host}[[:blank:]]\+%{amanda_user}[[:blank:]]\+amdump\" %{AMANDAHOMEDIR}/.amandahosts`" ] ; then
                echo "${host}   %{amanda_user} amdump" >>%{AMANDAHOMEDIR}/.amandahosts
        fi
done
chown %{amanda_user}:%{amanda_group} %{AMANDAHOMEDIR}/.amandahosts >>${TMPFILE} 2>&1
chmod 0600 %{AMANDAHOMEDIR}/.amandahosts >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

# SSH RSA key generation for amdump
KEYDIR="%{AMANDAHOMEDIR}/.ssh"
KEYFILE="id_rsa_amdump"
COMMENT="%{amanda_user}@server"
if [ ! -d ${KEYDIR} ] ; then
        if [ -f ${KEYDIR} ] ; then
                echo "`date +'%b %e %Y %T'`: Directory '${KEYDIR}' exists as a file.  Renaming to '${KEYDIR}.rpmsave'." >${TMPFILE}
                mv ${KEYDIR} ${KEYDIR}.rpmsave
                cat ${TMPFILE}
                cat ${TMPFILE} >>${INSTALL_LOG}
        fi
        echo "`date +'%b %e %Y %T'`: Creating directory '${KEYDIR}'." >${TMPFILE}
        mkdir ${KEYDIR} >>${TMPFILE} 2>&1
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_LOG}
fi
if [ ! -f ${KEYDIR}/${KEYFILE} ] ; then
        echo "`date +'%b %e %Y %T'`: Creating ssh RSA key in '${KEYDIR}/${KEYFILE}'" >${TMPFILE}
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_LOG}
        ssh-keygen -q -C $COMMENT -t rsa -f ${KEYDIR}/${KEYFILE} -N '' >>${TMPFILE} 2>&1
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_LOG}
fi
echo "`date +'%b %e %Y %T'`: Setting ownership and permissions for '${KEYDIR}' and '${KEYDIR}/${KEYFILE}*'" >${TMPFILE}
chown %{amanda_user}:%{amanda_group} ${KEYDIR} ${KEYDIR}/${KEYFILE}* >>${TMPFILE} 2>&1
chmod 0750 ${KEYDIR} >>${TMPFILE} 2>&1
chmod 0600 ${KEYDIR}/${KEYFILE}* >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

# SSH RSA key generation on client for amrecover
KEYDIR="%{AMANDAHOMEDIR}/.ssh"
KEYFILE="id_rsa_amrecover"
COMMENT="root@client"
if [ ! -d ${KEYDIR} ] ; then
        if [ -f ${KEYDIR} ] ; then
                echo "`date +'%b %e %Y %T'`: Directory '${KEYDIR}' exists as a file.  Renaming to '${KEYDIR}.rpmsave'." >${TMPFILE}
                mv ${KEYDIR} ${KEYDIR}.rpmsave >>${TMPFILE} 2>&1
                cat ${TMPFILE}
                cat ${TMPFILE} >>${INSTALL_LOG}
        fi
        echo "`date +'%b %e %Y %T'`: Creating directory '${KEYDIR}'." >${TMPFILE}
        mkdir ${KEYDIR} >>${TMPFILE} 2>&1
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_LOG}
fi
if [ ! -f ${KEYDIR}/${KEYFILE} ] ; then
        echo "`date +'%b %e %Y %T'`: Creating ssh RSA key in '${KEYDIR}/${KEYFILE}'" >${TMPFILE}
        ssh-keygen -q -C $COMMENT -t rsa -f ${KEYDIR}/${KEYFILE} -N '' >>${TMPFILE} 2>&1
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_LOG}
fi
echo "`date +'%b %e %Y %T'`: Setting permissions for '${KEYDIR}'" >${TMPFILE}
chown %{amanda_user}:%{amanda_group} ${KEYDIR} >>${TMPFILE} 2>&1
chmod 0750 ${KEYDIR} >>${TMPFILE} 2>&1
chmod 0600 ${KEYDIR}/${KEYFILE}* >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

# environment variables (~amandabackup/.profile)
echo "`date +'%b %e %Y %T'`: Checking for '%{AMANDAHOMEDIR}/.profile' and ensuring correct environment." >${TMPFILE}
if [ ! -f %{AMANDAHOMEDIR}/.profile ] ; then
        touch %{AMANDAHOMEDIR}/.profile >>${TMPFILE} 2>&1
fi
if [ -z "`grep PATH %{AMANDAHOMEDIR}/.profile | grep '%{SBINDIR}'`" ] ; then
        echo "export PATH=\"\$PATH:%{SBINDIR}\"" >>%{AMANDAHOMEDIR}/.profile 2>>${TMPFILE}
fi
echo "`date +'%b %e %Y %T'`: Setting ownership and permissions for '%{AMANDAHOMEDIR}/.profile'" >>${TMPFILE}
chown %{amanda_user}:%{amanda_group} %{AMANDAHOMEDIR}/.profile >>${TMPFILE} 2>&1
chmod 0640 %{AMANDAHOMEDIR}/.profile >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

echo "`date +'%b %e %Y %T'`: Sending anonymous distribution and version information to Zmanda" >> ${INSTALL_LOG}
if [ -x /usr/bin/wget ]; then 
        /usr/bin/wget -q -o /dev/null -O - --timeout=5 http://www.zmanda.com/amanda-tips.php\?version=%{amanda_version}\&os=%{disttag}%{distver}\&type=server 
fi

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
if [ "`id -u %{amanda_user} > /dev/null 2>&1 && echo 0 || echo 1`" != "0" ] ; then
        useradd -c "Amanda" -M -g %{amanda_group} -d %{AMANDAHOMEDIR} -s /bin/sh %{amanda_user}
        if [ %{dist} = "SuSE" ]; then
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
                echo "`date +'%b %e %Y %T'`:  that %{AMANDAHOMEDIR}/.amandahosts on your client systems" >>${TMPFILE}
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
        echo "`date +'%b %e %Y %T'`:  HOME:           %{AMANDAHOMEDIR}" >>${TMPFILE}
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

        if [ "`grep ^%{amanda_user} /etc/passwd|cut -d: -f6`" != "%{AMANDAHOMEDIR}" ] ; then
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! user 'amandabackup' home directory should be set to   !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! %{AMANDAHOMEDIR} Pl correct before using Amanda       !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
        else
                echo "`date +'%b %e %Y %T'`:  Verified Default home directory for user amandabackup" >>${TMPFILE}
        fi
        echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
        PASSWD_OK=0
fi
if [ -d %{AMANDAHOMEDIR} ] ; then
        echo -n "`date +'%b %e %Y %T'`:  Checking ownership of '%{AMANDAHOMEDIR}'... " >>${TMPFILE}
        if [ "`ls -dl %{AMANDAHOMEDIR} | awk '//{split($_,x); print x[3]}'`" = "%{amanda_user}" ] && \
           [ "`ls -dl %{AMANDAHOMEDIR} | awk '//{split($_,x); print x[4]}'`" = "%{amanda_group}" ] ; then
                echo "correct." >>${TMPFILE}
                VARLIB_OK=0
        else
                echo "incorrect!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  Please ensure that the directory '%{AMANDAHOMEDIR}' is owned by" >>${TMPFILE}
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

if [ -e /etc/xinetd.d ] && [ -d /etc/xinetd.d ] ; then
        if [ ! -f /etc/xinetd.d/amandaserver ] ; then
                cp %{AMANDAHOMEDIR}/example/xinetd.amandaserver /etc/xinetd.d/amandaserver
                chmod 0644 /etc/xinetd.d/amandaserver >>${TMPFILE} 2>&1
                if [ -f /etc/xinetd.d/amandaclient ] ; then
                        rm /etc/xinetd.d/amandaclient
                fi

                echo -n "`date +'%b %e %Y %T'`: Reloading xinetd configuration..." >${TMPFILE}
                if [ "%{xinetd_reload}" == "reload" ] ; then
                        /etc/init.d/xinetd %{xinetd_reload} >>${TMPFILE} 2>&1
                        ret_val=$?
                        if [ ${ret_val} -ne 0 ] ; then
                                echo -n "reload failed.  Attempting restart..." >>${TMPFILE}
                                /etc/init.d/xinetd restart >>${TMPFILE} 2>&1
                                ret_val=$?
                        fi
                else
                        /etc/init.d/xinetd %{xinetd_reload} >>${TMPFILE} 2>&1
                        ret_val=$?
                fi
                if [ ${ret_val} -eq 0 ] ; then
                        echo "success." >>${TMPFILE}
                        cat ${TMPFILE}
                        cat ${TMPFILE} >>${INSTALL_LOG}
                else
                        echo "failed.  Please check your system logs." >>${TMPFILE}
                        cat ${TMPFILE} 1>&2
                        cat ${TMPFILE} >>${INSTALL_ERR}
                fi
        fi
fi

echo "`date +'%b %e %Y %T'`: Installing '%{AMANDATES}'." >${TMPFILE}
ret_val=0
if [ ! -f %{AMANDATES} ] ; then
        touch %{AMANDATES} >>${TMPFILE} 2>&1
        ret_val=$?
        if [ ${ret_val} -eq 0 ]; then
                echo "`date +'%b %e %Y %T'`: The file '%{AMANDATES}' has been created." >>${TMPFILE}
        fi
fi
if [ ${ret_val} -eq 0 ]; then
        echo "`date +'%b %e %Y %T'`: Ensuring correct permissions for '%{AMANDATES}'." >>${TMPFILE}
        chown %{amanda_user}:%{amanda_group} %{AMANDATES} >>${TMPFILE} 2>&1
        chmod 0640 %{AMANDATES} >>${TMPFILE} 2>&1
fi
if [ ${ret_val} -eq 0 ]; then
        echo "`date +'%b %e %Y %T'`: '%{AMANDATES}' Installation successful." >>${TMPFILE}
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_LOG}
else
        echo "`date +'%b %e %Y %T'`: '%{AMANDATES}' Installation failed." >>${TMPFILE}
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_ERR}
fi

# Install .amandahosts to server
echo "`date +'%b %e %Y %T'`: Checking '%{AMANDAHOMEDIR}/.amandahosts' file." >${TMPFILE}
if [ ! -f %{AMANDAHOMEDIR}/.amandahosts ] ; then
        touch %{AMANDAHOMEDIR}/.amandahosts >>${TMPFILE} 2>&1
fi
for host in localhost localhost.localdomain ; do
        if [ -z "`grep \"^${host}[[:blank:]]\+root[[:blank:]]\+amindexd[[:blank:]]\+amidxtaped\" %{AMANDAHOMEDIR}/.amandahosts`" ] ; then
                echo "${host}   root amindexd amidxtaped" >>%{AMANDAHOMEDIR}/.amandahosts
        fi
        if [ -z "`grep \"^${host}[[:blank:]]\+%{amanda_user}[[:blank:]]\+amdump\" %{AMANDAHOMEDIR}/.amandahosts`" ] ; then
                echo "${host}   %{amanda_user} amdump" >>%{AMANDAHOMEDIR}/.amandahosts
        fi
done
chown %{amanda_user}:%{amanda_group} %{AMANDAHOMEDIR}/.amandahosts >>${TMPFILE} 2>&1
chmod 0600 %{AMANDAHOMEDIR}/.amandahosts >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

# Install amanda client configuration file
echo "`date +'%b %e %Y %T'`: Checking '%{SYSCONFDIR}/amanda/amanda-client.conf' file." >${TMPFILE}
if [ ! -f %{SYSCONFDIR}/amanda/amanda-client.conf ] ; then
        cp %{AMANDAHOMEDIR}/example/amanda-client.conf %{SYSCONFDIR}/amanda/amanda-client.conf >>${TMPFILE} 2>&1
fi
chown %{amanda_user}:%{amanda_group} %{SYSCONFDIR}/amanda/amanda-client.conf >>${TMPFILE} 2>&1
chmod 0600 %{SYSCONFDIR}/amanda/amanda-client.conf >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

# install am_passphrase file to server
echo "`date +'%b %e %Y %T'`: Checking '%{AMANDAHOMEDIR}/.am_passphrase' file." >${TMPFILE}
if [ ! -f %{AMANDAHOMEDIR}/.am_passphrase ] ; then
        echo "`date +'%b %e %Y %T'`: Create '%{AMANDAHOMEDIR}/.am_passphrase' file." >${TMPFILE}
        touch %{AMANDAHOMEDIR}/.am_passphrase >>${TMPFILE} 2>&1
        phrase=`echo $RANDOM | md5sum | awk '{print $1}'`
        echo ${phrase} >>%{AMANDAHOMEDIR}/.am_passphrase

        chown %{amanda_user}:%{amanda_group} %{AMANDAHOMEDIR}/.am_passphrase >>${TMPFILE} 2>&1
        chmod 0700 %{AMANDAHOMEDIR}/.am_passphrase >>${TMPFILE} 2>&1
fi
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

# Install .gnupg directory
echo "`date +'%b %e %Y %T'`: Installing '%{AMANDAHOMEDIR}/.gnupg'." >${TMPFILE}
ret_val=0
if [ ! -d %{AMANDAHOMEDIR}/.gnupg ] ; then
        echo "`date +'%b %e %Y %T'`: '%{AMANDAHOMEDIR}/.gnupg' will be created." >>${TMPFILE}
        mkdir %{AMANDAHOMEDIR}/.gnupg >>${TMPFILE} 2>&1
        ret_val=$?
        if [ ${ret_val} -eq 0 ]; then
                echo "`date +'%b %e %Y %T'`: The directory '%{AMANDAHOMEDIR}/.gnupg' created successfully." >>${TMPFILE}
        else
                echo "`date +'%b %e %Y %T'`: The directory '%{AMANDAHOMEDIR}/.gnupg' creation failed." >>${TMPFILE}
        fi
fi
if [ ${ret_val} -eq 0 ]; then
        echo "`date +'%b %e %Y %T'`: Ensuring correct permissions for '%{AMANDAHOMEDIR}/.gnupg'." >>${TMPFILE}
        chown %{amanda_user}:%{amanda_group} %{AMANDAHOMEDIR}/.gnupg >>${TMPFILE} 2>&1
        ret_val=$?
        if [ ${ret_val} -eq 0 ]; then
                chmod 700 %{AMANDAHOMEDIR}/.gnupg >>${TMPFILE} 2>&1
                ret_val=$?
        fi
fi
if [ ${ret_val} -eq 0 ]; then
        echo "`date +'%b %e %Y %T'`: '%{AMANDAHOMEDIR}/.gnupg' Installation successful." >>${TMPFILE}
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_LOG}
else
        echo "`date +'%b %e %Y %T'`: '%{AMANDAHOMEDIR}/.gnupg' Installation failed." >>${TMPFILE}
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_ERR}
fi

# SSH RSA key generation on server for amdump
KEYDIR="%{AMANDAHOMEDIR}/.ssh"
KEYFILE="id_rsa_amdump"
COMMENT="%{amanda_user}@server"
if [ ! -d ${KEYDIR} ] ; then
        if [ -f ${KEYDIR} ] ; then
                echo "`date +'%b %e %Y %T'`: Directory '${KEYDIR}' exists as a file.  Renaming to '${KEYDIR}.rpmsave'." >${TMPFILE}
                mv ${KEYDIR} ${KEYDIR}.rpmsave >>${TMPFILE} 2>&1
                cat ${TMPFILE}
                cat ${TMPFILE} >>${INSTALL_LOG}
        fi
        echo "`date +'%b %e %Y %T'`: Creating directory '${KEYDIR}'." >${TMPFILE}
        mkdir ${KEYDIR} >>${TMPFILE} 2>&1
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_LOG}
fi
if [ ! -f ${KEYDIR}/${KEYFILE} ] ; then
        echo "`date +'%b %e %Y %T'`: Creating ssh RSA key in '${KEYDIR}/${KEYFILE}'" >${TMPFILE}
        ssh-keygen -q -C $COMMENT -t rsa -f ${KEYDIR}/${KEYFILE} -N '' >>${TMPFILE} 2>&1
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_LOG}
fi
echo "`date +'%b %e %Y %T'`: Setting ownership and permissions for '${KEYDIR}' and '${KEYDIR}/${KEYFILE}*'" >${TMPFILE}
chown %{amanda_user}:%{amanda_group} ${KEYDIR} ${KEYDIR}/${KEYFILE}* >>${TMPFILE} 2>&1
chmod 0750 ${KEYDIR} >>${TMPFILE} 2>&1
chmod 0600 ${KEYDIR}/${KEYFILE}* >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

# SSH RSA key generation on client for amrecover
KEYDIR="%{AMANDAHOMEDIR}/.ssh"
KEYFILE="id_rsa_amrecover"
COMMENT="root@client"
if [ ! -d ${KEYDIR} ] ; then
        if [ -f ${KEYDIR} ] ; then
                echo "`date +'%b %e %Y %T'`: Directory '${KEYDIR}' exists as a file.  Renaming to '${KEYDIR}.rpmsave'." >${TMPFILE}
                mv ${KEYDIR} ${KEYDIR}.rpmsave >>${TMPFILE} 2>&1
                cat ${TMPFILE}
                cat ${TMPFILE} >>${INSTALL_LOG}
        fi
        echo "`date +'%b %e %Y %T'`: Creating directory '${KEYDIR}'." >${TMPFILE}
        mkdir ${KEYDIR} >>${TMPFILE} 2>&1
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_LOG}
fi
if [ ! -f ${KEYDIR}/${KEYFILE} ] ; then
        echo "`date +'%b %e %Y %T'`: Creating ssh RSA key in '${KEYDIR}/${KEYFILE}'" >${TMPFILE}
        ssh-keygen -q -C $COMMENT -t rsa -f ${KEYDIR}/${KEYFILE} -N '' >>${TMPFILE} 2>&1
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_LOG}
fi
echo "`date +'%b %e %Y %T'`: Setting ownership and permissions for '${KEYDIR}'" >${TMPFILE}
chown %{amanda_user}:%{amanda_group} ${KEYDIR} >>${TMPFILE} 2>&1
chmod 0750 ${KEYDIR} >>${TMPFILE} 2>&1
chmod 0600 ${KEYDIR}/${KEYFILE}* >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

# environment variables (~amandabackup/.profile)
echo "`date +'%b %e %Y %T'`: Checking for '%{AMANDAHOMEDIR}/.profile' and ensuring correct environment." >${TMPFILE}
if [ ! -f %{AMANDAHOMEDIR}/.profile ] ; then
        touch %{AMANDAHOMEDIR}/.profile >>${TMPFILE} 2>&1
fi
if [ -z "`grep PATH %{AMANDAHOMEDIR}/.profile | grep '%{SBINDIR}'`" ] ; then
        echo "export PATH=\"\$PATH:%{SBINDIR}\"" >>%{AMANDAHOMEDIR}/.profile 2>>${TMPFILE}
fi
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

echo "`date +'%b %e %Y %T'`: Setting ownership and permissions for '%{AMANDAHOMEDIR}/.profile'" >${TMPFILE}
chown %{amanda_user}:%{amanda_group} %{AMANDAHOMEDIR}/.profile >>${TMPFILE} 2>&1
chmod 0640 %{AMANDAHOMEDIR}/.profile >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

echo "`date +'%b %e %Y %T'`: Sending anonymous distribution and version information to Zmanda" >> ${INSTALL_LOG}
if [ -x /usr/bin/wget ]; then 
        /usr/bin/wget -q -o /dev/null -O - --timeout=5 http://www.zmanda.com/amanda-tips.php\?version=%{amanda_version}\&os=%{disttag}%{distver}\&type=server 
fi

echo "`date +'%b %e %Y %T'`: === Amanda backup server installation complete. ===" >${TMPFILE}

cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

if [ -f "${TMPFILE}" ]; then
        rm -f "${TMPFILE}" >>${TMPFILE} 2>&1
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
if [ "`id -u %{amanda_user} > /dev/null 2>&1 && echo 0 || echo 1`" != "0" ] ; then
        useradd -c "Amanda" -M -g %{amanda_group} -d %{AMANDAHOMEDIR} -s /bin/sh %{amanda_user} >>${TMPFILE} 2>&1
        if [ %{dist} = "SuSE" ]; then
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
                echo "`date +'%b %e %Y %T'`:  that %{AMANDAHOMEDIR}/.amandahosts on your client systems" >>${TMPFILE}
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
        echo "`date +'%b %e %Y %T'`:  HOME:           %{AMANDAHOMEDIR}" >>${TMPFILE}
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

        if [ "`grep ^%{amanda_user} /etc/passwd|cut -d: -f6`" != "%{AMANDAHOMEDIR}" ] ; then
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! user 'amandabackup' home directory should be set to   !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! %{AMANDAHOMEDIR} Pl correct before using Amanda       !!!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  !!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING! !!!" >>${TMPFILE}
        else
                echo "`date +'%b %e %Y %T'`:  Verified Default home directory for user amandabackup" >>${TMPFILE}
        fi
        echo "`date +'%b %e %Y %T'`:" >>${TMPFILE}
        PASSWD_OK=0
fi
if [ -d %{AMANDAHOMEDIR} ] ; then
        echo -n "`date +'%b %e %Y %T'`:  Checking ownership of '%{AMANDAHOMEDIR}'... " >>${TMPFILE}
        if [ "`ls -dl %{AMANDAHOMEDIR} | awk '//{split($_,x); print x[3]}'`" = "%{amanda_user}" ] && \
           [ "`ls -dl %{AMANDAHOMEDIR} | awk '//{split($_,x); print x[4]}'`" = "%{amanda_group}" ] ; then
                echo "correct." >>${TMPFILE}
                VARLIB_OK=0
        else
                echo "incorrect!" >>${TMPFILE}
                echo "`date +'%b %e %Y %T'`:  Please ensure that the directory '%{AMANDAHOMEDIR}' is owned by" >>${TMPFILE}
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

if [ -e /etc/xinetd.d ] && [ -d /etc/xinetd.d ] ; then
        if [ ! -f /etc/xinetd.d/amandaclient ] ; then
                cp %{AMANDAHOMEDIR}/example/xinetd.amandaclient /etc/xinetd.d/amandaclient

                echo -n "`date +'%b %e %Y %T'`: Reloading xinetd configuration..." >${TMPFILE}
                if [ "%{xinetd_reload}" == "reload" ] ; then
                        /etc/init.d/xinetd %{xinetd_reload} >>${TMPFILE} 2>&1
                        ret_val=$?
                        if [ ${ret_val} -ne 0 ] ; then
                                echo -n "reload failed.  Attempting restart..." >>${TMPFILE}
                                /etc/init.d/xinetd restart >>${TMPFILE} 2>&1
                                ret_val=$?
                        fi
                else
                        /etc/init.d/xinetd %{xinetd_reload} >>${TMPFILE} 2>&1
                        ret_val=$?
                fi
                if [ ${ret_val} -eq 0 ] ; then
                        echo "success." >>${TMPFILE}
                        cat ${TMPFILE}
                        cat ${TMPFILE} >>${INSTALL_LOG}
                else
                        echo "failed.  Please check your system logs." >>${TMPFILE}
                        cat ${TMPFILE}
                        cat ${TMPFILE} >>${INSTALL_LOG}
                fi
        fi
fi

echo "`date +'%b %e %Y %T'`: Installing '%{AMANDATES}'." >${TMPFILE}
ret_val=0
if [ ! -f %{AMANDATES} ] ; then
        touch %{AMANDATES} >>${TMPFILE} 2>&1
        ret_val=$?
        if [ ${ret_val} -eq 0 ]; then
                echo "`date +'%b %e %Y %T'`: The file '%{AMANDATES}' has been created." >>${TMPFILE}
        fi
fi
if [ ${ret_val} -eq 0 ]; then
        echo "`date +'%b %e %Y %T'`: Ensuring correct permissions for '%{AMANDATES}'." >>${TMPFILE}
        chown %{amanda_user}:%{amanda_group} %{AMANDATES} >>${TMPFILE} 2>&1
        chmod 0640 %{AMANDATES} >>${TMPFILE} 2>&1
fi
if [ ${ret_val} -eq 0 ]; then
        echo "`date +'%b %e %Y %T'`: '%{AMANDATES}' Installation successful." >>${TMPFILE}
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_LOG}
else
        echo "`date +'%b %e %Y %T'`: '%{AMANDATES}' Installation failed." >>${TMPFILE}
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_ERR}
fi

# Install .amandahosts to client
echo "`date +'%b %e %Y %T'`: Checking '%{AMANDAHOMEDIR}/.amandahosts' file." >${TMPFILE}
if [ ! -f %{AMANDAHOMEDIR}/.amandahosts ] ; then
        touch %{AMANDAHOMEDIR}/.amandahosts >>${TMPFILE} 2>&1
fi
for host in localhost localhost.localdomain ; do
                if [ -z "`grep \"^${host}[[:blank:]]\+\" %{AMANDAHOMEDIR}/.amandahosts | grep \"[[:blank:]]\+%{amanda_user}[[:blank:]]\+amdump\"`" ] ; then
                        echo "${host}   %{amanda_user} amdump" >>%{AMANDAHOMEDIR}/.amandahosts
                fi
done
chown %{amanda_user}:%{amanda_group} %{AMANDAHOMEDIR}/.amandahosts >>${TMPFILE} 2>&1
chmod 0600 %{AMANDAHOMEDIR}/.amandahosts >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

# Install amanda client configuration file
echo "`date +'%b %e %Y %T'`: Checking '%{SYSCONFDIR}/amanda/amanda-client.conf' file." >${TMPFILE}
if [ ! -f %{SYSCONFDIR}/amanda/amanda-client.conf ] ; then
        cp %{AMANDAHOMEDIR}/example/amanda-client.conf %{SYSCONFDIR}/amanda/amanda-client.conf >>${TMPFILE} 2>&1
fi
chown %{amanda_user}:%{amanda_group} %{SYSCONFDIR}/amanda/amanda-client.conf >>${TMPFILE} 2>&1
chmod 0600 %{SYSCONFDIR}/amanda/amanda-client.conf >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

# Install .gnupg directory
echo "`date +'%b %e %Y %T'`: Installing '%{AMANDAHOMEDIR}/.gnupg'." >${TMPFILE}
ret_val=0
if [ ! -d %{AMANDAHOMEDIR}/.gnupg ] ; then
        echo "`date +'%b %e %Y %T'`: '%{AMANDAHOMEDIR}/.gnupg' will be created." >>${TMPFILE}
        mkdir %{AMANDAHOMEDIR}/.gnupg >>${TMPFILE} 2>&1
        ret_val=$?
        if [ ${ret_val} -eq 0 ]; then
                echo "`date +'%b %e %Y %T'`: The directory '%{AMANDAHOMEDIR}/.gnupg' created successfully." >>${TMPFILE}
        else
                echo "`date +'%b %e %Y %T'`: The directory '%{AMANDAHOMEDIR}/.gnupg' creation failed." >>${TMPFILE}
        fi
fi
if [ ${ret_val} -eq 0 ]; then
        echo "`date +'%b %e %Y %T'`: Ensuring correct permissions for '%{AMANDAHOMEDIR}/.gnupg'." >>${TMPFILE}
        chown %{amanda_user}:%{amanda_group} %{AMANDAHOMEDIR}/.gnupg >>${TMPFILE} 2>&1
        ret_val=$?
        if [ ${ret_val} -eq 0 ]; then
                chmod 700 %{AMANDAHOMEDIR}/.gnupg >>${TMPFILE} 2>&1
                ret_val=$?
        fi
fi
if [ ${ret_val} -eq 0 ]; then
        echo "`date +'%b %e %Y %T'`: '%{AMANDAHOMEDIR}/.gnupg' Installation successful." >>${TMPFILE}
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_LOG}
else
        echo "`date +'%b %e %Y %T'`: '%{AMANDAHOMEDIR}/.gnupg' Installation failed." >>${TMPFILE}
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_ERR}
fi

# SSH RSA key generation on client for amrecover
KEYDIR="%{AMANDAHOMEDIR}/.ssh"
KEYFILE="id_rsa_amrecover"
COMMENT="root@client"
if [ ! -d ${KEYDIR} ] ; then
        if [ -f ${KEYDIR} ] ; then
                echo "`date +'%b %e %Y %T'`: Directory '${KEYDIR}' exists as a file.  Renaming to '${KEYDIR}.rpmsave'." >${TMPFILE}
                mv ${KEYDIR} ${KEYDIR}.rpmsave >>${TMPFILE} 2>&1
                cat ${TMPFILE}
                cat ${TMPFILE} >>${INSTALL_LOG}
        fi
        echo "`date +'%b %e %Y %T'`: Creating directory '${KEYDIR}'." >${TMPFILE}
        mkdir ${KEYDIR} >>${TMPFILE} 2>&1
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_LOG}
fi
if [ ! -f ${KEYDIR}/${KEYFILE} ] ; then
        echo "`date +'%b %e %Y %T'`: Creating ssh RSA key in '${KEYDIR}/${KEYFILE}'" >${TMPFILE}
        ssh-keygen -q -C $COMMENT -t rsa -f ${KEYDIR}/${KEYFILE} -N '' >>${TMPFILE} 2>&1
        cat ${TMPFILE}
        cat ${TMPFILE} >>${INSTALL_LOG}
fi
echo "`date +'%b %e %Y %T'`: Setting permissions for '${KEYDIR}' and '${KEYDIR}/${KEYFILE}*'" >${TMPFILE}
chown %{amanda_user}:%{amanda_group} ${KEYDIR} >>${TMPFILE} 2>&1
chmod 0750 ${KEYDIR} >>${TMPFILE} 2>&1
chmod 0600 ${KEYDIR}/${KEYFILE}* >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

# environment variables (~amandabackup/.profile)
echo "`date +'%b %e %Y %T'`: Checking for '%{AMANDAHOMEDIR}/.profile' and ensuring correct environment." >${TMPFILE}
if [ ! -f %{AMANDAHOMEDIR}/.profile ] ; then
        touch %{AMANDAHOMEDIR}/.profile >>${TMPFILE} 2>&1
fi
if [ -z "`grep PATH %{AMANDAHOMEDIR}/.profile | grep '%{SBINDIR}'`" ] ; then
        echo "export PATH=\"\$PATH:%{SBINDIR}\"" >>%{AMANDAHOMEDIR}/.profile 2>>${TMPFILE}
fi
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}
echo "`date +'%b %e %Y %T'`: Setting ownership and permissions for '%{AMANDAHOMEDIR}/.profile'" >${TMPFILE}
chown %{amanda_user}:%{amanda_group} %{AMANDAHOMEDIR}/.profile >>${TMPFILE} 2>&1
chmod 0640 %{AMANDAHOMEDIR}/.profile >>${TMPFILE} 2>&1
cat ${TMPFILE}
cat ${TMPFILE} >>${INSTALL_LOG}

echo "`date +'%b %e %Y %T'`: Sending anonymous distribution and version information to Zmanda" >> ${INSTALL_LOG}
if [ -x /usr/bin/wget ]; then 
        /usr/bin/wget -q -o /dev/null -O - --timeout=5 http://www.zmanda.com/amanda-tips.php\?version=%{amanda_version}\&os=%{disttag}%{distver}\&type=client 
fi

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
# Notes:  Do not use wildcards on directories not wholly owned by amanda.  An
# uninstall of the software will attempt to delete whatever matches here.
%files backup_client
%defattr(0755,%{amanda_user},%{amanda_group},0755)
%{AMLIBEXECDIR}
%{AMLIBDIR}
%{PERLSITELIB}/auto/Amanda
%defattr(4750,root,disk)
%{AMLIBEXECDIR}/application/amgtar
%{AMLIBEXECDIR}/application/amstar
%{AMLIBEXECDIR}/calcsize
%{AMLIBEXECDIR}/killpgrp
%{AMLIBEXECDIR}/rundump
%{AMLIBEXECDIR}/runtar
%defattr(0750,%{amanda_user},%{amanda_group})
%{LOGDIR}
%{SBINDIR}/amaespipe
%{SBINDIR}/amcryp*
%{SBINDIR}/amgpgcrypt
%{SBINDIR}/amoldrecover
%{SBINDIR}/amrecover
%defattr(0644,%{amanda_user},%{amanda_group},0755)
%{LOCALSTATEDIR}/amanda
%{PERLSITELIB}/Amanda
%{SYSCONFDIR}/amanda
%docdir %{MANDIR}
%{MANDIR}/man5/amanda.conf.5.gz
%{MANDIR}/man5/amanda-client.conf.5.gz
%{MANDIR}/man7/amanda-devices.7.gz
%{MANDIR}/man7/amanda-applications.7.gz
%{MANDIR}/man7/amanda-scripts.7.gz
%{MANDIR}/man8/amaespipe.8.gz
%{MANDIR}/man8/amanda.8.gz
%{MANDIR}/man8/amcheckdump.8.gz
%{MANDIR}/man8/amcrypt*
%{MANDIR}/man8/amgpgcrypt.8.gz
%{MANDIR}/man8/amrecover.8.gz
%{AMLIBEXECDIR}/amcat.awk
%{AMANDAHOMEDIR}/gnutar-lists
%doc %{AMANDAHOMEDIR}/amanda-release
%doc %{AMANDAHOMEDIR}/example/xinetd.amandaclient
%doc %{AMANDAHOMEDIR}/example/xinetd.amandaserver
%doc %{AMANDAHOMEDIR}/example/amanda-client.conf
%doc %{AMANDAHOMEDIR}/template.d/README
%doc %{AMANDAHOMEDIR}/template.d/dumptypes
%defattr(0644,root,root,0755)
%doc %{DATADIR}/amanda

%files backup_server
%defattr(0755,%{amanda_user},%{amanda_group})
%{SYSCONFDIR}/amanda
%{AMLIBEXECDIR}
%{AMLIBDIR}
%{PERLSITELIB}/Amanda
%{PERLSITELIB}/auto/Amanda
%{AMANDAHOMEDIR}
%{LOCALSTATEDIR}/amanda
%{SBINDIR}/am*
%defattr(4750,root,disk)
%{AMLIBEXECDIR}/application/amgtar
%{AMLIBEXECDIR}/application/amstar
%{AMLIBEXECDIR}/calcsize
%{AMLIBEXECDIR}/killpgrp
%{AMLIBEXECDIR}/rundump
%{AMLIBEXECDIR}/runtar
%{AMLIBEXECDIR}/dumper
%{AMLIBEXECDIR}/planner
%{SBINDIR}/amcheck
%defattr(0750,%{amanda_user},%{amanda_group})
%{LOGDIR}
# Files in standard dirs must be listed explicitly
%{SBINDIR}/activate-devpay
%{SBINDIR}/amaespipe
%{SBINDIR}/amcrypt
%{SBINDIR}/amcrypt-ossl
%{SBINDIR}/amcrypt-ossl-asym
%{SBINDIR}/amcryptsimple
%{SBINDIR}/amgpgcrypt
%{SBINDIR}/amoldrecover
%{SBINDIR}/amrecover
%defattr(0644,%{amanda_user},%{amanda_group})
%{AMLIBEXECDIR}/amcat.awk
%{AMLIBEXECDIR}/amplot.awk
%{AMLIBEXECDIR}/amplot.g
%{AMLIBEXECDIR}/amplot.gp
%docdir %{MANDIR}
%{MANDIR}/man5/am*
%{MANDIR}/man5/disklist.5.gz
%{MANDIR}/man5/tapelist.5.gz
%{MANDIR}/man7/am*
%{MANDIR}/man8/am*
%{MANDIR}/man8/script-email.8.gz
%doc %{AMANDAHOMEDIR}/amanda-release
%docdir %{AMANDAHOMEDIR}/example
%docdir %{AMANDAHOMEDIR}/template.d
%defattr(0644,root,root,0755)
%doc %{DATADIR}/amanda

# --- ChangeLog

%changelog
* Mon Sep 15 2008 Dan Locks <dwlocks at zmanda dot com> 2.6.1alpha
- Added detection of CentOS 4 and 5 as suggested by dswartz
- graceful failure when Distro/version is not detected correctly
* Thu Jun 12 2008 Dan Locks <dwlocks at zmanda dot com> 2.6.1alpha
- install amgtar and amstar suid root
* Mon Jun 09 2008 Dan Locks <dwlocks at zmanda dot com> 2.6.1alpha
- Replaced individual SBINDIR/am... entries with SBINDIR/am* in %%files
* Fri May 02 2008 Dan Locks <dwlocks at zmanda dot com>
- Changed instances of ${ to %%{ where applicable
* Tue Mar 11 2008 Dan Locks <dwlocks at zmanda dot com>
- fixed many rpmlint complaints
- added --quiet to configure statements
- added PERLSITELIB to definitions section and perl files to %%files section
* Wed Feb 13 2008 Dan Locks <dwlocks at zmanda dot com>
- added an environment check for PKG_CONFIG_PATH
- added PKG_CONFIG_PATH conditional to handle cross comp on FC8 (environment 
  var is used if provided)
* Fri Feb 01 2008 Dan Locks <dwlocks at zmanda dot com>
- Removed amplot executable and manpages from client installation
- Added amcheckdump.8 manpage
- Fixed %%{LOCALSTATEDIR}/amanda dir creation.
* Wed Jan 23 2008  Dan Locks <dwlocks at zmanda dot com>
- Change %%{SYSCONFDIR}/amanda/amandates to %%{LOCALSTATEDIR}/amanda/amandates,
  and added %%{LOCALSTATEDIR}/amanda to the files lists.
* Mon Jan 14 2008  Dan Locks <dwlocks at zmanda dot com>
- Updates for perlified amanda, file location moves, gpg setup.
* Tue Nov  13 2007 Paddy Sreenivasan <paddy at zmanda dot com>
- Added SYSCONFDIR to client rpm
- Set xinetd and amanda-client.conf configuration files as part of postinstall
* Thu Nov  8 2007 Dan Locks <dwlocks at zmanda dot com>
- Added Linux distribution detection
* Wed Nov 7 2007 Paddy Sreenivasan <paddy at zmanda dot com>
- Added amserverconfig, amaddclient, amgpgcrypt, amcryptsimple and libamdevice.
- Added amanda configuration template files
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

