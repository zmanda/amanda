/*
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 *
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

%module "Amanda::Util"
%include "amglue/amglue.swg"
%include "exception.i"

%include "Amanda/Util.pod"

%{
#include <unistd.h>
#include "amglue.h"
#include "debug.h"
#include "full-read.h"
#include "full-write.h"
#include "fsusage.h"
#include "stream.h"
/* use a relative path here to avoid conflicting with Perl's util.h. */
#include "../common-src/util.h"
#include "file.h"
#include "sockaddr-util.h"
%}

%perlcode %{

use Amanda::Debug qw(:init);
use Amanda::Config qw(:getconf);
use warnings;
use Carp;
use POSIX qw( :fcntl_h :errno_h );
use POSIX qw( strftime );
use Amanda::Constants;
use Amanda::Process;

# private package variables
my $_pname;
my $_ptype;
my $_pcontext;

sub setup_application {
    my ($name, $type, $context) = @_;

    # sanity check
    croak("no name given") unless ($name);
    croak("no type given") unless ($type);
    croak("no context given") unless ($context);

    # store these as perl values
    $_pname = $name;
    $_ptype = $type;
    $_pcontext = $context;

    # and let the C side know about them too
    set_pname($name);
    set_ptype($type);
    set_pcontext($context);

    safe_cd(); # (also sets umask)
    check_std_fds();

    # set up debugging, now that we have a name, type, and context
    debug_init();

    # ignore SIGPIPE
    $SIG{'PIPE'} = 'IGNORE';
}

sub finish_setup {
    my ($running_as) = @_;

    my $config_name = Amanda::Config::get_config_name();

    if ($config_name) {
	dbrename($config_name, $_ptype);
    }

    check_running_as($running_as);
}

sub finish_application {
    dbclose();
}

sub version_opt {
    print "$_pname-$Amanda::Constants::VERSION\n";
    exit 0;
}

%}
char *get_original_cwd(void);
amglue_export_tag(util, get_original_cwd);

%perlcode %{
sub safe_env {
    my %rv = %ENV;

    delete @rv{qw(IFS CDPATH ENV BASH_ENV LANG)};

    # delete all LC_* variables
    for my $var (grep /^LC_/, keys %rv) {
        delete $rv{$var};
    }

    return %rv;
}

%}

amglue_add_flag_tag_fns(running_as_flags);
amglue_add_constant(RUNNING_AS_ANY, running_as_flags);
amglue_add_constant(RUNNING_AS_ROOT, running_as_flags);
amglue_add_constant(RUNNING_AS_DUMPUSER, running_as_flags);
amglue_add_constant(RUNNING_AS_DUMPUSER_PREFERRED, running_as_flags);
amglue_add_constant(RUNNING_AS_CLIENT_LOGIN, running_as_flags);
amglue_add_constant(RUNNING_AS_UID_ONLY, running_as_flags);
amglue_copy_to_tag(running_as_flags, constants);

amglue_add_enum_tag_fns(pcontext_t);
amglue_add_constant(CONTEXT_DEFAULT, pcontext_t);
amglue_add_constant(CONTEXT_CMDLINE, pcontext_t);
amglue_add_constant(CONTEXT_DAEMON, pcontext_t);
amglue_add_constant(CONTEXT_SCRIPTUTIL, pcontext_t);
amglue_copy_to_tag(pcontext_t, constants);

%perlcode %{
sub full_read {
    my ($fd, $count) = @_;
    my @bufs;

    while ($count > 0) {
	my $b;
	my $n_read = POSIX::read($fd, $b, $count);
	if (!defined $n_read) {
	    next if ($! == EINTR);
	    return undef;
	} elsif ($n_read == 0) {
	    last;
	}
	push @bufs, $b;
	$count -= $n_read;
    }

    return join('', @bufs);
}

sub full_write {
    my ($fd, $buf, $count) = @_;
    my $total = 0;

    while ($count > 0) {
	my $n_written = POSIX::write($fd, $buf, $count);
	if (!defined $n_written) {
	    next if ($! == EINTR);
	    return undef;
	} elsif ($n_written == 0) {
	    last;
	}

	$count -= $n_written;
	$total += $n_written;

	if ($count) {
	    $buf = substr($buf, $n_written);
	}
    }

    return $total;
}

sub skip_quoted_string {
    my $str = shift;

    chomp $str;
    my $iq = 0;
    my $i = 0;
    my $c = substr $str, $i, 1;
    while ($c ne "" && !($iq == 0 && $c =~ /\s/)) {
	if ($c eq '"') {
	    $iq = !$iq;
	} elsif ($c eq '\\') {
	    $i++;
	}
	$i++;
	$c = substr $str, $i, 1;
    }
    my $quoted_string = substr $str, 0, $i;
    my $remainder     = undef;
    if (length($str) > $i) {
	$remainder    = substr $str, $i+1;
    }

    return ($quoted_string, $remainder);
}

sub split_quoted_string_friendly {
    my $str = shift;
    my @result;

    chomp $str;
    $str =~ s/^\s+//;
    while ($str) {
	(my $elt, $str) = skip_quoted_string($str);
	push @result, unquote_string($elt);
	$str =~ s/^\s+// if $str;
    }

    return @result;
}

%}

amglue_export_ok(slurp);
amglue_export_ok(burp);
amglue_export_ok(safe_overwrite_file);

%perlcode %{

sub slurp {
    my $file = shift @_;
    local $/;

    open my $fh, "<", $file or croak "can't open $file: $!";
    my $data = <$fh>;
    close $fh;

    return $data;
}

sub burp {
    my $file = shift @_;
    open my $fh, ">", $file or croak "can't open $file: $!";
    print $fh @_;
}

sub safe_overwrite_file {
    my ( $filename, $contents ) = @_;

    my $tmpfname = "$filename." . time;
    open my $tmpfh, ">", $tmpfname or die "open: $!";

    print $tmpfh $contents;
    (fsync($tmpfh) == 0) or die "fsync: $!";
    return rename $tmpfname, $filename;
}

%}

%typemap (in) GPtrArray * {
    AV *av;
    guint len;
    int i;

    if (!SvROK($input) || SvTYPE(SvRV($input)) != SVt_PVAV) {
	SWIG_exception(SWIG_TypeError, "Expected an arrayref");
    }
    av = (AV *)SvRV($input);

    len = av_len(av)+1; /* av_len(av) is like $#av */
    $1 = g_ptr_array_sized_new(len);
    for (i = 0; i < len; i++) {
	SV **elt = av_fetch(av, i, 0);
	if (!elt || !SvPOK(*elt)) {
	    SWIG_exception(SWIG_TypeError, "Non-string in arrayref");
	}
	g_ptr_array_add($1, SvPV_nolen(*elt)); /* TODO: handle unicode here */
    }
}
%typemap (freearg) GPtrArray * {
    g_ptr_array_free($1, FALSE);
}

%typemap (out) GPtrArray * {
    if ($1) {
	guint i;
	for (i = 0; i < $1->len; i++) {
	    $result = sv_2mortal(newSVpv(g_ptr_array_index($1, i), 0));
	    argvi++;
	}
	g_ptr_array_free($1, TRUE);
    } else {
	$result = &PL_sv_undef;
	argvi++;
    }
}

/* for split_quoted_strings */
%typemap(out) gchar ** {
    gchar **iter;

    if ($1) {
	/* Count the DeviceProperties */
	EXTEND(SP, g_strv_length($1)); /* make room for return values */

	/* Note that we set $result several times. the nature of
	 * SWIG's wrapping is such that incrementing argvi points
	 * $result to the next location in perl's argument stack.
	 */

	for (iter = $1; *iter; iter++) {
	    $result = sv_2mortal(newSVpv(*iter, 0));
	    g_free(*iter);
	    argvi++;
	}
	g_free($1);
    }
}

%rename(hexencode) hexencode_string;
char *hexencode_string(char *);
%rename(hexdecode) perl_hexdecode_string;
char *perl_hexdecode_string(char *);
%{
char *perl_hexdecode_string(const char *str) {
    GError *err = NULL;
    char *tmp;
    tmp = hexdecode_string(str, &err);
    if (err) {
        g_free(tmp);
        croak_gerror("Amanda util: hexdecode", &err);
    }
    return tmp;
}
%}
amglue_export_tag(encoding, hexencode hexdecode);

%newobject sanitise_filename;
char *sanitise_filename(char *inp);
%newobject quote_string;
char *quote_string(char *);
%newobject unquote_string;
char *unquote_string(char *);
GPtrArray *expand_braced_alternates(char *);
%newobject collapse_braced_alternates;
char *collapse_braced_alternates(GPtrArray *source);
%newobject split_quoted_strings;
gchar **split_quoted_strings(const gchar *string);
amglue_export_tag(quoting, quote_string unquote_string skip_quoted_string
		sanitise_filename split_quoted_strings split_quoted_strings_friendly);
amglue_export_tag(alternates, expand_braced_alternates collapse_braced_alternates);

%perlcode %{

sub generate_timestamp {
    # this corresponds to common-src/timestamp.c's get_proper_stamp_from_time
    if (getconf($CNF_USETIMESTAMPS)) {
	return strftime "%Y%m%d%H%M%S", localtime;
    } else {
	return strftime "%Y%m%d", localtime;
    }
}

sub built_with_component {
    my ($component) = @_;
    my @components = split / +/, $Amanda::Constants::AMANDA_COMPONENTS;
    return grep { $_ eq $component } @components;
}

%}

/* interface to gnulib's fsusage */
%typemap(in,numinputs=0) (struct fs_usage *fsp)
    (struct fs_usage fsu) {
    bzero(&fsu, sizeof(fsu));
    $1 = &fsu;
}

%typemap(argout) (struct fs_usage *fsp) {
    SV *sv;
    HV *hv;

    /* if there was an error, assume that fsu_blocksize isn't changed,
     * and return undef. */
    if ($1->fsu_blocksize) {
	SP += argvi; PUTBACK; /* save the perl stack so amglue_newSVi64 doesn't kill it */
	hv = (HV *)sv_2mortal((SV *)newHV());
	hv_store(hv, "blocksize", 9, amglue_newSVi64($1->fsu_blocksize), 0);
	hv_store(hv, "blocks", 6, amglue_newSVi64($1->fsu_blocks), 0);
	hv_store(hv, "bfree", 5, amglue_newSVi64($1->fsu_bfree), 0);
	hv_store(hv, "bavail", 6, amglue_newSVi64($1->fsu_bavail), 0);
	hv_store(hv, "bavail_top_bit_set", 18, newSViv($1->fsu_bavail_top_bit_set), 0);
	hv_store(hv, "files", 5, amglue_newSVi64($1->fsu_files), 0);
	hv_store(hv, "ffree", 5, amglue_newSVi64($1->fsu_ffree), 0);

	$result = newRV((SV *)hv);
	SPAGAIN; SP -= argvi;
	argvi++;
    }
}

%rename(get_fs_usage) get_fs_usage_;
%inline %{
void get_fs_usage_(const char *file, struct fs_usage *fsp)
{
    int rv = get_fs_usage(file, NULL, fsp);
    if (rv == -1)
	/* signal an error to the typemap */
	fsp->fsu_blocksize = 0;
}
%}

/*
 * Operations that should be in Perl but aren't
 */

int fsync(int fd);

/* Perl's fcntl only operates on file handles */
%inline %{
int
set_blocking(int fd, gboolean blocking)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0)
	return flags;
    if (blocking)
	flags &= ~O_NONBLOCK;
    else
	flags |= O_NONBLOCK;
    flags = fcntl(fd, F_SETFL, flags);
    if (flags < 0)
	return flags;
    return 0;
}
%}

/*
 * Locking (see amflock.h)
 */

/* SWIG prepends the struct name to the member function name, which
 * conflicts with the underlying function names */

typedef struct file_lock {
    %extend {
	%newobject file_lock;
	file_lock(const char *filename) {
	    return file_lock_new(filename);
	}

	~locked_data() {
	    file_lock_free(self);
	}

	int lock();
	int lock_wr();
	int lock_rd();
	int unlock();
	int locked();

	%typemap(in) (const char *data, size_t len) {
	    $1 = SvPV($input, $2);
	}

	int write(const char *data, size_t len);

	/* get the data as an SV */
	%typemap(out) (SV *) { $result = $1; argvi++; };
	SV *data() {
	    if (self->data) {
		return newSVpvn(self->data, self->len);
	    } else {
		return &PL_sv_undef;
	    }
	}
	%typemap(out) (SV *);
    }
} file_lock;

%perlcode %{

sub is_pid_alive {
    my ($pid) = shift;

    return 1 if $pid == $$;

    my $Amanda_process = Amanda::Process->new(0);

    $Amanda_process->load_ps_table();
    my $alive = $Amanda_process->process_alive($pid);
    return $alive;

}
%}

/* Interesting story: Perl added a sv_rvweaken function in 5.6.0 (or earlier?), but
 * did not include this functionality in Scalar::Util until later.  It doesn't make
 * much sense, does it? */
amglue_export_ok(weaken_ref)
%typemap(in) SV *rv "$1 = $input;"
%inline %{
void weaken_ref(SV *rv) {
    sv_rvweaken(rv);
}
%}

%rename(gettimeofday) gettimeofday_for_perl;
%inline %{
static guint64 gettimeofday_for_perl(void)
{
    GTimeVal t;
    g_get_current_time(&t);
    return (guint64)t.tv_sec * G_USEC_PER_SEC + (guint64)t.tv_usec;
}
%}

void openbsd_fd_inform(void);

/*
 * Streams
 *
 * TODO: this should move to Amanda::Security when the rest of the Security API
 * is available from Perl.
 */

enum { AF_INET };
enum { STREAM_BUFSIZE };
%typemap(in, numinputs=0) in_port_t *port_ARGOUT (in_port_t port) {
    $1 = &port;
}
%typemap(argout) in_port_t *port_ARGOUT {
    $result = sv_2mortal(newSViv(*$1));
    argvi++;
}
/* avoid BigInts for socket fd's */
%{ typedef int socketfd; %}
%typemap(out) socketfd {
    $result = sv_2mortal(newSViv($1));
    argvi++;
}
socketfd stream_server(int family, in_port_t *port_ARGOUT, size_t sendsize,
		  size_t recvsize, gboolean privileged);

socketfd stream_accept(int fd, int timeout, size_t sendsize, size_t recvsize);

%newobject check_security_fd;
%rename(check_security) check_security_fd;
%inline %{
char *check_security_fd(int fd, char *userstr)
{
    socklen_t_equiv i;
    struct sockaddr_in addr;
    char *errstr;

    /* get the remote address */
    i = SIZEOF(addr);
    if (getpeername(fd, (struct sockaddr *)&addr, &i) == -1) {
	return g_strdup_printf("getpeername: %s", strerror(errno));
    }

    /* require IPv4 and not port 20 -- apparently this was a common attack
     * vector for much older Amandas */
    if ((addr.sin_family != (sa_family_t)AF_INET)
		|| (ntohs(addr.sin_port) == 20)) {
	return g_strdup_printf("connection rejected from %s family %d port %d",
             inet_ntoa(addr.sin_addr), addr.sin_family, htons(addr.sin_port));
    }

    /* call out to check_security */
    if (!check_security((sockaddr_union *)&addr, userstr, 0, &errstr))
	return errstr;

    return NULL;
}
%}
amglue_export_ok(
	stream_server stream_accept check_security);
amglue_export_tag(constants,
	$AF_INET $STREAM_BUFSIZE);

%perlcode %{

# these functions were verified to work similarly to those in
# common-src/tapelist.c - they pass the same tests, at least.

sub marshal_tapespec {
    my ($filelist) = @_;
    my @filelist = @$filelist; # make a copy we can wreck
    my @specs;

    while (@filelist) {
	my $label = shift @filelist;
	my $files = shift @filelist;

	$label =~ s/([\\:;,])/\\$1/g;
	push @specs, "$label:" . join(",", @$files);
    }
    return join(";", @specs);
}

sub unmarshal_tapespec {
    my ($tapespec) = @_;
    my @filelist;

    # detect a non-tapespec string for special handling; in particular, a string
    # without an unquoted : followed by digits and commas at the end.  The easiest
    # way to do this is to replace every quoted character with a dummy, then look
    # for the colon and digits.
    my $tmp = $tapespec;
    $tmp =~ s/\\([\\:;,])/X/g;
    if ($tmp !~ /:[,\d]+$/) {
	# ok, it doesn't end with the right form, so unquote it and return it
	# with filenum 0
	$tapespec =~ s/\\([\\:;,])/$1/g;
	return [ $tapespec, [ 0 ] ];
    }

    # use a lookbehind to mask out any quoted ;'s
    my @volumes = split(/(?<!\\);/, $tapespec);
    for my $vol (@volumes) {
	my ($label, $files) = ($vol =~ /(.+):([\d,]+)/);

	$label =~ s/\\([\\:;,])/$1/g;
	push @filelist, $label;

	my @files = split(/,/, $files);
	@files = map { $_+0 } @files;
	@files = sort { $a <=> $b } @files;
	push @filelist, \@files;
    }

    return \@filelist;
}

%}

amglue_export_ok(
    match_host match_disk match_datestamp match_level
);

gboolean match_host(char *pat, char *value);
gboolean match_disk(char *pat, char *value);
gboolean match_datestamp(char *pat, char *value);
gboolean match_level(char *pat, char *value);


/* -------------------------------------------------------------------------
 * Functions below this line are only meant to be called within this module;
 * do not call them externally. */

void set_pname(char *name);
char *get_pname();
void set_ptype(char *type);
char *get_ptype();
void set_pcontext(pcontext_t context);
pcontext_t get_pcontext();
void safe_cd(void);

void check_running_as(running_as_flags who);

/* Check that fd's 0, 1, and 2 are open, calling critical() if not.
 */
%perlcode %{
sub check_std_fds {
    fcntl(STDIN, F_GETFD, 0) or critical("Standard input is not open");
    fcntl(STDOUT, F_GETFD, 0) or critical("Standard output is not open");
    fcntl(STDERR, F_GETFD, 0) or critical("Standard error is not open");
}

%}
