+++
title = "Patches"
date = 2019-03-23T15:37:35+05:30
weight = 5
chapter = true
hidden = true
+++

*<sub><sub>Amanda.org is an open source site. Please refer the [TO DO LIST](/to_do) to contribute to this site.</sub></sub>*

The AMANDA Patches Page
=======================

This page contains a list of important patches to some of the older
releases of AMANDA.

*Please be aware of the fact that the current release of AMANDA is
2.6.1p2 or higher, so think twice about applying some old patch. We, the
AMANDA Core Team, strongly recommend to update to the latest stable
release !!!*

The links below contain pointers to messages posted to the AMANDA
mailing lists. Please read the messages for additional information on
the problems they fix and any additional action you must take for the
patch to work.

You may use GNU patch in order to apply these patches. It is available
at [the GNU anonymous FTP site](ftp://ftp.gnu.org/pub/gnu) or at any of
its mirrors, in a file named `patch-`*release-number*`.tar.gz`.

Most patches are `application/x-patch` MIME attachments of messages
posted to AMANDA mailing lists, and this is the recommended form of
posting patches, because such patches can be downloaded directly from
the archives (not as of May 23, but eGroups is working to fix the
problem). Some have been included in the mail text, so you have to click
on the `Source` button and save the page. With a bit of luck, you'll be
able to use the `.html` file as input for `patch`, but it may require
some tweaking. If you cut and paste a patch file from the browser
window, make sure to run `patch -l` so that the patch program does not
get confused because of differences of tabs and spaces.

A `-p0` or `-p1` switch has become almost mandatory in latest releases
of GNU patch; if you don't issue a `-p` switch, patch may guess
incorrectly the file to patch, and you'll get rejected junks for files
such as `INSTALL` and `Makefile.am`.

For applying a patch that contain `Index:` lines, setting the
environment variable `POSIXLY_CORRECT` before running the patch command
may help getting the patch applied without human intervention. Thanks to
[Evan Champion](mailto:<evanc@synapse.net>) for pointing this out. A
detailed explanation of why this helps can be found in the GNU patch
man-page.

Patches for older releases are no longer listed here.

Patches for AMANDA 2.4.5
------------------------

-   [amanda-2.4.5-amoverview.patch](2.4.5/amanda-2.4.5-amoverview.patch):
    This patch by [Orion Poplawski](mailto:<orion@cora.nwra.com) adds
    new options to amoverview to toggle new output-columns. The patch
    also includes additions to the amoverview-manpage.
-   [amanda-2.4.5-planner\_incrlargerthantape.patch](2.4.5/amanda-2.4.5-planner_incrlargerthantape.patch):
    This patch by [Paul Bijnens](mailto:<paul.bijnens@xplanation.com)
    solves the problem that a delayed-full-because-too-large turns into
    an incremental-also-too-large dump.
-   [amanda-2.4.5-reporter\_notrunchostname.patch](2.4.5/amanda-2.4.5-reporter_notrunchostname.patch):
    This patch by [Paul Bijnens](mailto:<paul.bijnens@xplanation.com)
    fixes the truncated columns in AMANDA reports.
-   [amanda-2.4.5-span\_split\_V7.2\_2.4.5.gz](2.4.5/amanda-2.4.5-span_split_V7.2_2.4.5.gz):
    This patch by [John Stange](mailto:<building@cs.umd.edu) adds the
    ability to span DLEs over more than one tape.
-   [amanda-2.4.5-tcp\_wrappers.patch](2.4.5/amanda-2.4.5-tcp_wrappers.patch):
    This patch by [Michael Weiser](mailto:<michael@weiser.dinsnail.net)
    adds support for tcp\_wrappers (libwrap).

Patches for AMANDA 2.4.2p2
--------------------------

-   [advfs.patch](2.4.2p2/advfs.diff): This corrects a problem with
    detecting the advfs file system type (e.g. on Irix and Tru64) and
    picking the right dump program to use. It also cleans up some
    problems with linux device name determinations. (Posted on Apr 12,
    2001)
-   [stream\_client.patch](2.4.2p2/stream_client.diff): This corrects a
    problem with amrecover reporting "did not get a reserved port" when
    --with-portrange had been used with ./configure. (Posted on Apr 2,
    2002)

Patches for packages used by AMANDA:
------------------------------------

-   [samba2-largefs.patch](http://www.egroups.com/group/amanda-hackers/1101.html?):
    an update of the patch `samba-largefs.patch`, distributed with
    AMANDA 2.4.1p1, for Samba up to release 2.0.3. This patch is already
    installed in the Samba CVS tree, so it will no longer be needed in
    newer releases of Samba.
-   [restore.diff](http://www.egroups.com/group/amanda-users/11432.html?):
    Samba 2.0.2, and probably 2.0.0 too, have trouble when restoring
    files whose sizes are exact multiples of 512 bytes, and may have
    trouble reading from pipes. This patch by [Bob
    Boehmer](mailto:boehmer@worldnet.att.net) fixes these problems. Note
    that tar-files produced before this patch are still usable, as the
    problems addressed by this patch are in the restore code only. The
    problem is already fixed in Samba 2.0.3.
-   [sambatar.diff](http://www.egroups.com/group/amanda-users/mg2115354892.html?):
    Samba 1.9.18p5 up to 1.9.18p10 will print messages to stdout, even
    when asked to create a tar-file and write to stdout. Since AMANDA
    asks SAMBA to create tar-files to stdout, if you do not apply this
    patch in SAMBA, your backups will be useless. Problem reported by
    [Ronny Blomme](mailto:<Ronny.Blomme@elis.rug.ac.be>). According to
    [Todd Pfaff](mailto:pfaff@McMaster.CA), this problem is fixed in
    Samba 2.0.0.
-   [samba-gtar.diff](ftp://ftp.AMANDA.org/pub/amanda/maillist-archives/amanda-users/www/users/Apr-Jun.1998/msg00208.html):
    Samba 1.9.18p4 (and probably previous 1.9.18 versions) won't read
    tar-files with gnutar-style long filenames, even ones produced by
    itself. Patch by [Rob Riggs](mailto:<rob@devilsthumb.com>). Fixed in
    Samba 1.9.18p5. (Apr 12, 1998)
-   Samba 1.9.17 and higher will print incorrect size information for
    large (\> 2GB) filesystems on hosts whose ints are 32bits. If your C
    compiler supports \``unsigned long long`'s and `printf()` supports
    "`%llu`" for printing them, you should apply `samba-largefs.patch`,
    available in AMANDA distributions since release 2.4.0b5. Anyway,
    beware large SMB filesystems: some MS-Windows hosts were reported
    (in Samba mailing lists) to randomly corrupt such filesystems, but
    then, this has nothing to do with Samba or AMANDA.
-   GNU tar 1.12: there are two known problems (described below) that
    can be fixed by applying `tar-1.12.patch`, available in AMANDA
    distributions since release 2.4.0b5.
    -   On SunOS 4.1.3, HP/UX and possibly other systems, GNU tar 1.12
        will report incorrect output sizes, because printf does not
        understand "`%llu`".
    -   GNU tar 1.12 would report `Bad file number` error messages at
        estimate-time for sparse files.
-   GNU tar 1.13: even though the second problem in release 1.12 is
    already fixed in 1.13, the other is only partially fixed. Besides,
    there are a couple of other problems in release 1.13, related with
    exclude patterns and restoring, so its use is not recommended.
    Hopefully, all of these problems will be fixed in GNU tar 1.14. If
    you want to use something past 1.12,
    [1.13.25](ftp://alpha.gnu.org/pub/gnu/tar/tar-1.13.25.tar.gz)
    appears to be stable.
-   GNU tar 1.13.18: This [patch](tar-1.13.18.diff) fixes two problems
    in tar-1.13.18:
    -   A workaround for a bug in fnmatch from glibc.
    -   A bug that can cause a core dump.

* * * * *

This page is maintained by the AMANDA Core Development Team.

Please report changes and/or additions to the
[AMANDA-hackers](mailto:<AMANDA-hackers@AMANDA.org>) mailing list.

* * * * *

*Last updated: Date: 2014/12/12 18:44:43 *
