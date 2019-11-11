# Amanda Backup Software
## (The Advanced Maryland Automatic Network Disk Archiver)

- [License](#license)
- [What is Amanda?](#what-is-amanda)
- [Requirements](#requirements)
- [Supported Systems](#supported-systems)
- [Download](#download)
- [Running it](#running-it)
- [Community](#community)

## License
Copyright (c) 1991-1998 University of Maryland at College Park
Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
All Rights Reserved.

PLEASE NOTE: THIS SOFTWARE IS BEING MADE AVAILABLE "AS-IS".  We make
no warranties that it will work for you.  As such there is no support
available other than users helping each other on the Amanda mailing
lists or forums. Formal support may be available through vendors.

[TOP](#amanda-backup-software)

## What is Amanda?
Amanda is a backup system designed to backup and archive many
computers on a network to disk, tape changer/drive or cloud storage.

Here are some features of Amanda:

  - Written in C and Perl.

  - Freely distributable source and executable. University of Maryland
    (BSD style) license and GPL.

  - Built on top of standard backup software: Unix dump/restore, GNU
    Tar and other archival tools. It is extensible to support new
    archival applications.

  - Open file and tape formats. If necessary, you can use standard
    tools like mt and GNU Tar to recover data.

  - Backs up 32 and 64 bit Windows machines.

  - Will back up multiple machines in parallel to a holding disk. Once
    a dump is complete, Amanda will copy finished dumps one by one to
    virtual tape on a disk or tape as fast as it can.  For example:

    - A 30 GB backup to virtual tape on disk may take less than 75
      minutes.

    - A 41GB backup to AIT5 (25MB/s transfer) may take 40 minutes of
      tape time.

  - Maintains a catalog of files being backed up and their location on
    the media.

  - Does tape management: e.g. Amanda will not overwrite the wrong
    tape.

  - For a restore, tells you what tapes you need, and finds the proper
    backup image on the tape for you.

  - Supports tape changers via a generic interface.  Easily
    customizable to any type of tape library, carousel, robot,
    stacker, or virtual tape that can be controlled via the unix
    command line.

  - Device API provides a pluggable interface to storage
    devices. Bundled drivers support tapes and virtual tapes on disk,
    DVD-RW, RAIT, and Amazon S3. The bundled amvault can then copy to
    removable media for off-site (D2D2T) or cloud storage (D2D2C).

  - Supports secure communication between server and client using
    OpenSSH, allowing secure backup of machines in a DMZ or out in the
    Internet.

  - Can encrypt backup archives on Amanda client or on Amanda server
    using GPG or any encryption program.

  - Can compress backup archives before sending or after sending over
    the network, with compress, gzip or a custom program.

  - Supports Kerberos 5 security, including encrypted dumps.

  - Recovers gracefully from errors, including down or hung machines.

  - Reports results in detail, including all errors, via email.

  - Dynamically adjusts the backup schedule to keep within
    constraints: no more juggling by hand when adding disks and
    computers to your network.

  - Backup normalization: Amanda schedules full and incremental
    backups so you don't have to, and so as to spread the load across
    the backup cycle. Amanda will intelligently promote a backup level
    in case it is determines that is optimal for resources.

  - Includes a pre-run checker program, that conducts sanity checks on
    both the tape server host and all the client hosts (in parallel),
    and will send an e-mail report of any problems that could cause
    the backups to fail.

  - IPv6 friendly.

  - Runs transparently from cron as needed.

  - Span tapes, i.e. if a single backup is too large for one tape,
    Amanda will split it and put the pieces on multiple tapes
    automatically.

  - Application API allows custom backups for applications such as
    relational databases, or for special file systems.

  - Executes user-provided pre- and post-backup scripts, for,
    e.g. enforcing database referential integrity.

  - Award-winning! Including: Linux Journal Readers' Choice Award.

  - Lots of other options; Amanda is very configurable.

[TOP](#amanda-backup-software)

## Requirements
Amanda requires a host that has access to disks (local, NAS or SAN) or
a large capacity tape drive or library. All modern tape formats,
e.g. LTO, EXABYTE, DAT or DLT are supported. This becomes the "backup
server host".  All the computers you are going to backup are the
"backup client hosts".  The server host can also be a client host.

Amanda works best with one or more large "holding disk" partitions on
the server host available to it for buffering dumps before writing to
tape.  The holding disk allows Amanda to run backups in parallel to
the disk, only writing them to tape when the backup is finished.  Note
that the holding disk is not required: without it Amanda will run
backups sequentially to the tape drive.  Running it this way may not
be optimal for performance, but still allows you to take advantage of
Amanda's other features.

As a rule of thumb, for best performance the holding disk should be
larger than the dump output from your largest disk partitions.  For
example, if you are backing up some terabyte disks that compress down
to 500 GB, then you'll want at least 500 GB on your holding disk.  On
the other hand, if those terabyte drives are partitioned into 50 GB
filesystems, they'll probably compress down to 25 GB and you'll only
need that much on your holding disk.  Amanda will perform better with
larger holding disks.

Actually, Amanda will still work if you have full dumps that are
larger than the holding disk: Amanda will send those dumps directly to
tape one at a time.  If you have many such dumps you will be limited
by the dump speed of those machines.

[TOP](#amanda-backup-software)

## Supported systems
Amanda should run on any modern Unix system that supports dump or GNU
tar, has sockets and inetd (or a replacement such as xinetd), and
either system V shared memory, or BSD mmap implemented.

In particular, Amanda has been compiled, and the client side tested on
the following systems:

        AIX 3.2 and 4.1
        BSDI BSD/OS 2.1 and 3.1
        DEC OSF/1 3.2 and 4.0
        FreeBSD 6, 7 and 8
        GNU/Linux 2.6 on x86, m68k, alpha, sparc, arm and powerpc
        HP-UX 9.x and 10.x (x >= 01)
        IRIX 6.5.2 and up
        NetBSD 1.0
        Nextstep 3 (\*)
        OpenBSD 2.5 x86, sparc, etc (ports available)
        Solaris 10
        Ultrix 4.2
        Mac OS X 10
        Windows: XP Pro (Server pack 2), 2003 server, Vista, 2008
                server R2, Windows 7 (\*)

(\*) The Amanda server side is known to run on all of the other
machines except on those marked with an asterisk.

Backup operations can be CPU and Memory intensive (e.g. for
compression and encryption operations). It is recommended that you
have a server class CPU in the backup server.

[TOP](#amanda-backup-software)

## Download
Amanda, including its source tree, is on SourceForge:

        http://sourceforge.net/projects/amanda

Or see
        http://www.amanda.org/download.php

Most Linux distributions include amanda rpms or debian packages
pre-built for various architectures. Pre-built binaries are also
available at:

        http://www.zmanda.com/download-amanda.php

[TOP](#amanda-backup-software)

## Running it

Read the file docs/INSTALL.  There are a variety of steps, from
compiling Amanda to installing it on the backup server host and the
client machines.

    docs/INSTALL        contains general installation instructions.
    docs/NEWS           details new features in each release.

You can read Amanda documentation at:

        http://www.amanda.org

and at the Amanda wiki:

        http://wiki.zmanda.com

[TOP](#amanda-backup-software)

## Community

You can get Amanda help and questions answered from the mailing lists and
Amanda forums:

==> To join a mailing list, DO NOT, EVER, send mail to that list.  Send
    mail to *LISTNAME*-request@amanda.org, or amanda-lists@amanda.org
    with the following line in the body of the message:
        subscribe *LISTNAME* *YOUR_EMAIL_ADDRESS*

    You will receive an email acknowledging your subscription. Keep
    it. Should you ever wish to depart our company, it has unsubscribe
    and other useful information.

    amanda-announce
        The amanda-announce mailing list is for important announcements
        related to the Amanda Network Backup Manager package, including new
        versions, contributions, and fixes.  NOTE: the amanda-users list is
        itself on the amanda-announce distribution, so you only need to
        subscribe to one of the two lists, not both.
        To subscribe, send a message to amanda-announce-request@amanda.org.

    amanda-users
        The amanda-users mailing list is for questions and general discussion
        about the Amanda Network Backup Manager.  NOTE: the amanda-users list
        is itself on the amanda-announce distribution, so you only need to
        subscribe to one of the two lists, not both.
        To subscribe, send a message to amanda-users-request@amanda.org.

    amanda-hackers
        The amanda-hackers mailing list is for discussion of the
        technical details of the Amanda package, including extensions,
        ports, bugs, fixes, and alpha testing of new versions.
        To subscribe, send a message to amanda-hackers-request@amanda.org.

Amanda forums: http://forums.zmanda.com

Amanda Platform Experts: http://wiki.zmanda.com/index.php/Platform_Experts

Backup, Share and Enjoy,
The Amanda Development Team

[TOP](#amanda-backup-software)
