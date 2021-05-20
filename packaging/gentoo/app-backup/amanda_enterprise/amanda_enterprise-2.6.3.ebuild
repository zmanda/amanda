# Copyright 1999-2007 Gentoo Foundation
# Distributed under the terms of the GNU General Public License v2
# $Header: /var/cvsroot/gentoo-x86/app-backup/amanda/amanda-2.5.2_p1-r3.ebuild,v 1.1 2007/09/06 20:35:34 robbat2 Exp $

inherit eutils

DESCRIPTION="Amanda Network Backup and Archiving software"
HOMEPAGE="http://www.zmanda.com/"
SRC_URI="${P}.tar.gz"
LICENSE="as-is" # see http://wiki.zmanda.com/index.php/Amanda_Copyright
SLOT="0"
KEYWORDS="amd64 x86"
RDEPEND="sys-libs/readline
	           virtual/inetd
	           sys-apps/gawk
	           app-arch/tar
	           dev-lang/perl
			   dev-libs/glib
	           app-arch/dump
	           net-misc/openssh
	           dev-libs/glib
	           samba? ( net-fs/samba )
	           !minimal? ( virtual/mailx
	                   app-arch/mt-st
	                   sys-block/mtx
	                   sci-visualization/gnuplot
	                   app-crypt/aespipe
	                   app-crypt/gnupg )"

DEPEND="${RDEPEND}"
RESTRICT="fetch"

IUSE="debug minimal ipv6"

# Zmanda-specific knobs and stuff
AMANDAHOMEDIR=/var/lib/amanda
LOGDIR=/var/log/amanda
amanda_user=amandabackup
amanda_group=disk
LOGDIR=/var/log/amanda
udpportrange="700,740"
tcpportrange="11000,11040"
low_tcpportrange="700,710"

pkg_nofetch() {
	einfo "Please download ${SRC_URI} from the Zmanda Network downloads"
	einfo "page and place it in ${DISTDIR}."
}

pkg_setup() {
	   # Add '${amanda_user}'
	   enewuser "${amanda_user}" "-1" "/bin/bash" "${AMANDAHOMEDIR}" "${amanda_group}"
}

src_compile() {
	   # fix bug #36316
	   addpredict /var/cache/samba/gencache.tdb

	   local myconf
	   cd ${S}

	   # override a gentoo default; this may change in 2.6.4
	   myconf="${myconf} --localstatedir=/var" 

	   myconf="${myconf} --with-tape-server=localhost"
	   myconf="${myconf} --with-index-server=localhost"
	   myconf="${myconf} --with-user=${amanda_user}"
	   myconf="${myconf} --with-group=${amanda_group}"
	   myconf="${myconf} --with-gnutar=/bin/tar"
	   myconf="${myconf} --with-gnutar-listdir=${AMANDAHOMEDIR}/gnutar-lists"
	   myconf="${myconf} --with-udpportrange=${udpportrange}"
	   myconf="${myconf} --with-tcpportrange=${tcpportrange}"
	   myconf="${myconf} --with-low-tcpportrange=${low_tcpportrange}"
	   myconf="${myconf} --with-debugging=${LOGDIR}"
	   myconf="${myconf} --with-fqdn"
	   myconf="${myconf} --with-bsd-security"
	   myconf="${myconf} --with-ssh-security"
	   myconf="${myconf} --with-bsdudp-security"
	   myconf="${myconf} --with-bsdtcp-security"

	   # THIS HAS TO BE REMOVED FOR 2.6.4!!
	   myconf="${myconf} --libexecdir=/usr/lib/amanda"

	   use ipv6 || myconf="${myconf} --without-ipv6"

	   # Client only, as requested in bug #127725
	   use minimal && myconf="${myconf} --without-server"

	   econf ${myconf} || die "econf failed!"
	   emake -j1 || die "emake failed!"
}

src_install() {
	einfo "Doing stock install"
	make DESTDIR=${D} install || die

	# docs
	einfo "Installing documentation"
	dodoc AUTHORS C* INSTALL NEWS README || die

	# env.d file for cfgpro
	einfo "Installing env.d file"
	doenvd "${FILESDIR}/99amanda" || die

	# install one of the two xinetd scripts
	insinto /etc/xinetd.d
	if use minimal; then
		newins "${S}/example/xinetd.amandaclient" amanda || die
	else
		newins "${S}/example/xinetd.amandaserver" amanda || die
	fi

	# make sure a few dirs exist
	keepdir "${AMANDAHOMEDIR}/gnutar-lists" || die
	fowners ${amanda_user}:${amanda_group} "${AMANDAHOMEDIR}/gnutar-lists" || die

	keepdir "${LOGDIR}" || die
	fowners ${amanda_user}:${amanda_group} "${LOGDIR}" || die

	# amanda version info
	amanda_version_info="Amanda Enterprise Edition - version ${PV}"
	echo "${amanda_version_info}" > "${T}/amanda-release"
	insinto ${AMANDAHOMEDIR}
	doins "${T}/amanda-release" || die

	# .gnupg needs to exist and have specific perms
	keepdir "${AMANDAHOMEDIR}/.gnupg" || die
	fowners ${amanda_user}:${amanda_group} "${AMANDAHOMEDIR}/.gnupg" || die
	fperms 700 "${AMANDAHOMEDIR}/.gnupg" || die

	# set up amanda-client.conf
	einfo "Setting up amanda-client.conf"
	insinto /etc/amanda
	doins "${S}/example/amanda-client.conf" || die

	# set up a basic .amandahosts; the user will probably customize
	# this, and config protection will detect the conflict.
	einfo "Setting up .amandahosts"
	for host in localhost localhost.localdomain ; do 
		if ! use minimal; then
			echo "${host}   root amindexd amidxtaped" >>${T}/amandahosts || die
		fi
		echo "${host}   ${amanda_user} amdump" >>${T}/amandahosts || die
	done
	insinto ${AMANDAHOMEDIR}
	newins ${T}/amandahosts .amandahosts || die
	fowners ${amanda_user}:${amanda_group} ${AMANDAHOMEDIR}/.amandahosts || die
	fperms 0600 ${AMANDAHOMEDIR}/.amandahosts || die

	einfo "Setting up .profile"
	echo 'PATH="$PATH:/usr/sbin"' >> ${T}/profile
	insinto ${AMANDAHOMEDIR}
	newins ${T}/profile .profile || die
	fowners ${amanda_user}:${amanda_group} ${AMANDAHOMEDIR}/.profile || die
	fperms 0640 ${AMANDAHOMEDIR}/.profile || die
}

pkg_postinst() {
	# set up amandates

	# this isn't a configuration file (and we'll be moving it out of /etc/
	# soon), but it has to exist.

	if [ ! -f /etc/amandates ]; then
		einfo "Creating /etc/amandates"
		touch /etc/amandates || die
		chown ${amanda_user}:${amanda_group} /etc/amandates || die
		chmod 0640 /etc/amandates || die
	fi

	# same here -- we want to create this for the user's convenience, but
	# only if it doesn't exist.  We *do not* want to include it in any 
	# binary packages!

	if [ ! -f ${AMANDAHOMEDIR}/.am_passphrase ] ; then
		einfo "Creating Semi-random am_passpharase -- feel free to change it!"

		touch ${AMANDAHOMEDIR}/.am_passphrase
		phrase=`echo $RANDOM | md5sum | awk '{print $1}'`
		echo ${phrase} >>${AMANDAHOMEDIR}/.am_passphrase

		chown ${amanda_user}:${amanda_group} ${AMANDAHOMEDIR}/.am_passphrase
		chmod 0700 ${AMANDAHOMEDIR}/.am_passphrase
	fi

	# TODO: generate an ssh key when we support SSH
}
