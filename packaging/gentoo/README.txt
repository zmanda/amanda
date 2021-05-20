Because Gentoo is a source-based distribution, there is no notion of "building
a package".  Instead, this directory contains the ebuilds and ancillary
material required to build Amanda Enterprise Edition on a Gentoo machine.

The process is like this (assuming some familiarity with portage):

Add app-backup/amanda_enterprise to the appropriate place in a portage
overlay, e.g., /usr/local/portage/app-backup/amanda_enterprise.  This could
potentially be a subversion checkout if you want to be able to update.

Place amanda_enterprise-x.y.z.tar.gz in /usr/portage/distfiles.  This file is
not publicly downloadable, so we can't give portage a URL for it.

Then just emerge amanda_enterprise.  Everything should fall into place.
