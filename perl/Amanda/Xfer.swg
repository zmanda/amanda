/*
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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

%module "Amanda::Xfer"
%include "amglue/amglue.swg"
%include "exception.i"
%include "cstring.i"
%import "Amanda/MainLoop.swg"

%include "Xfer.pod"

%{
#include "glib-util.h"
#include "amxfer.h"
#include "amanda.h"
#include "sockaddr-util.h"
%}

/* The SWIGging of the transfer architecture.
 *
 * The C layer of the transfer architecture exposes some structs, which are
 * arranged through GObject magic into a class hierarchy.  It also exposes
 * regular C functions which are intended to act as methods on these structs.
 * Furthermore, it exposes Perl callbacks (via Amanda::MainLoop) with
 * parameters involving objects of these classes.
 *
 * SWIG doesn't support callbacks very well, and makes it particularly
 * difficult to represent a GObject class hierarchy.  Rather than try to "make
 * it fit" into SWIG, this module uses custom typemaps and perl/C conversions
 * to get all of this stuff right in the first place.
 *
 * For Xfer objects, we define two functions, new_sv_for_xfer and xfer_from_sv,
 * which create a new SV for an Xfer object, and subsequently extract a pointer
 * to the object from the SV.  The SV is both blessed and tied to the
 * Amanda::Xfer::Xfer class, in which all of the method calls are defined, and
 * which defines a DESTROY method that calls xfer_unref.
 *
 * XferElements are similar, but we have the added challenge of representing
 * subclasses with appropriate perl subclasses.  The solution is to tag each C
 * class with a perl class name, and use that name when blessing a new SV.
 *
 * Finally, XMsgs are reflected entirely into perl hashrefs, in the interest of
 * efficiency.
 */

/*
 * Initialization
 */

%init %{
    /* We need GType and GThread initialized to use xfers */
    glib_init();
%}

/*
 * Constants
 */

amglue_add_enum_tag_fns(xfer_status);
amglue_add_constant(XFER_INIT, xfer_status);
amglue_add_constant(XFER_START, xfer_status);
amglue_add_constant(XFER_RUNNING, xfer_status);
amglue_add_constant(XFER_DONE, xfer_status);
amglue_copy_to_tag(xfer_status, constants);

amglue_add_enum_tag_fns(xmsg_type);
amglue_add_constant(XMSG_INFO, xmsg_type);
amglue_add_constant(XMSG_ERROR, xmsg_type);
amglue_add_constant(XMSG_DONE, xmsg_type);
amglue_add_constant(XMSG_CANCEL, xmsg_type);
amglue_add_constant(XMSG_PART_DONE, xmsg_type);
amglue_add_constant(XMSG_READY, xmsg_type);
amglue_copy_to_tag(xmsg_type, constants);

/*
 * Wrapping machinery
 */

%{
/* Given an XMsg, return a hashref representing the message as a pure-perl
 * object.  The object is new, has refcount 1, and is totally independent of
 * the underlying XMsg.
 *
 * Reflecting the XMsg directly into Perl avoids the need to reference-count
 * the XMsg objects themselves, which can simply be freed after a callback
 * completes.  The overhead of creating a hash is likely equivalent to or
 * less than the overhead that would be consumed with SWIG's swig_$field_get
 * accessors, assuming that perl code examines most of the fields in a message.
 *
 * @param msg: the message to represent
 * @returns: a perl SV
 */
static SV *
new_sv_for_xmsg(
    XMsg *msg)
{
    static HV *amanda_xfer_msg_stash = NULL;
    HV *hash = newHV();
    SV *rv = newRV_noinc((SV *)hash);

    /* bless the rv as an Amanda::Xfer::Msg object */
    if (!amanda_xfer_msg_stash) {
	amanda_xfer_msg_stash = gv_stashpv("Amanda::Xfer::Msg", GV_ADD);
    }
    sv_bless(rv, amanda_xfer_msg_stash);

    /* TODO: consider optimizing by precomputing the hash values of
     * the keys? */

    /* elt */
    hv_store(hash, "elt", 3, new_sv_for_xfer_element(msg->elt), 0);

    /* type */
    hv_store(hash, "type", 4, newSViv(msg->type), 0);

    /* type */
    hv_store(hash, "version", 7, newSViv(msg->version), 0);

    /* message */
    if (msg->message)
	hv_store(hash, "message", 7, newSVpv(msg->message, 0), 0);

    /* successful */
    hv_store(hash, "successful", 10, newSViv(msg->successful), 0);

    /* eom */
    hv_store(hash, "eom", 3, newSViv(msg->eom), 0);

    /* eof */
    hv_store(hash, "eof", 3, newSViv(msg->eof), 0);

    /* size */
    hv_store(hash, "size", 4, amglue_newSVu64(msg->size), 0);

    /* duration */
    hv_store(hash, "duration", 8, newSVnv(msg->duration), 0);

    /* partnum */
    hv_store(hash, "partnum", 7, amglue_newSVu64(msg->partnum), 0);

    /* fileno */
    hv_store(hash, "fileno", 6, amglue_newSVu64(msg->fileno), 0);

    return rv;
}
%}

%typemap(in) Xfer * {
    $1 = xfer_from_sv($input);
}

%typemap(in) XferElement * {
    $1 = xfer_element_from_sv($input);
}

%typemap(out) Xfer * {
    $result = sv_2mortal(new_sv_for_xfer($1));
    argvi++;
}

%typemap(out) XferElement * {
    $result = sv_2mortal(new_sv_for_xfer_element($1));
    argvi++;
}

%typemap(newfree) Xfer * {
    xfer_unref($1);
}

%typemap(newfree) XferElement * {
    xfer_element_unref($1);
}

/*
 * Xfer functions
 */

/* A typemap for the input to the Xfer constructor, a.k.a. xfer_new */
%typemap(in,numinputs=1) (XferElement **elementlist, unsigned int nelements) {
    AV *av;
    unsigned int i;

    /* check that it's an arrayref */
    if (!SvROK($input) || SvTYPE(SvRV($input)) != SVt_PVAV) {
	SWIG_exception(SWIG_TypeError, "Expected an arrayref");
    }
    av = (AV *)SvRV($input);

    /* allocate memory for $1 */
    $2 = av_len(av)+1; /* av_len(av) is like $#av */
    $1 = g_new(XferElement *, $2);

    /* extract the underlying XferElement objects and add pointers to
     * them, "borrowing" the caller's references for the moment. */
    for (i = 0; i < $2; i++) {
	SV **sv = av_fetch(av, i, 0);
	XferElement *elt = sv? xfer_element_from_sv(*sv):NULL;

	if (!elt) {
	    SWIG_exception(SWIG_TypeError, "Expected an arrayref of Amanda::Xfer::Element objects");
	}
	$1[i] = elt;
    }
}

%typemap(freearg) (XferElement **elementlist, unsigned int nelements) {
    /* free the element vector allocated in the (in) typemap */
    g_free($1);
}

%newobject xfer_new;
Xfer *xfer_new(XferElement **elementlist, unsigned int nelements);
void xfer_unref(Xfer *);
xfer_status xfer_get_status(Xfer *xfer);
char *xfer_repr(Xfer *xfer);
void xfer_start(Xfer *xfer, gint64 offset, gint64 size);
void xfer_cancel(Xfer *xfer);
/* xfer_get_source is implemented below */

%inline %{
/* SWIG wants to treat this as a function */
#define xfer_get_status(xfer) ((xfer)->status)
%}

/* upgrade the start method to optionally take a callback, which is
 * passed to the GSource's set_callback */
%perlcode %{
sub xfer_start_with_callback {
    my ($xfer, $cb, $offset, $size) = @_;
    if (defined $cb) {
	my $releasing_cb = sub {
	    my ($src, $msg, $xfer) = @_;
	    my $done = $msg->{'type'} == $XMSG_DONE;
	    $src->remove() if $done;
	    $cb->(@_);
	    $cb = undef if $done; # break potential reference loop
	};
	$xfer->get_source()->set_callback($releasing_cb);
    }
    $offset = 0 if !defined $offset;
    $size = 0 if !defined $size;
    xfer_start($xfer, $offset, $size);
}
%}

/* Change the callback */
%perlcode %{
sub xfer_set_callback {
    my ($xfer, $cb) = @_;
    if (defined $cb) {
	my $releasing_cb = sub {
	    my ($src, $msg, $xfer) = @_;
	    my $done = $msg->{'type'} == $XMSG_DONE;
	    $src->remove() if $done;
	    $cb->(@_);
	    $cb = undef if $done; # break potential reference loop
       };
	$xfer->get_source()->set_callback($releasing_cb);
    } else {
	$xfer->get_source()->set_callback(undef);
    }
}
%}

/*
 * XferElement functions
 *
 * Some of these methods are not intended to be used from Perl; they are annotated
 * as "private".
 */

void xfer_element_unref(XferElement *elt); /* (wrap the macro, above) */
/* xfer_element_link_to -- private */
char *xfer_element_repr(XferElement *elt);
/* xfer_element_set_size -- private */
/* xfer_element_start -- private */
/* xfer_element_cancel -- private */

%inline %{
static gboolean same_elements(
	XferElement *a,
	XferElement *b)
{
    return a == b;
}
%}

/* subclass constructors */

/* N.B. When adding new classes, ensure that the class_init function
 * sets perl_class to the appropriate value. */

%newobject xfer_source_random;
XferElement *xfer_source_random(
    guint64 length,
    guint32 seed);

guint32 xfer_source_random_get_seed(
    XferElement *self);

%typemap(in) (void * pattern, size_t pattern_length) {
 size_t len;
 char * pat;

 pat = SvPV($input, len);
 $1 = g_memdup(pat, len);
 $2 = len;
}

%typemap(in) (gchar **argv) {
    AV *av;
    unsigned int len;
    unsigned int i;

    /* check that it's an arrayref */
    if (!SvROK($input) || SvTYPE(SvRV($input)) != SVt_PVAV) {
	SWIG_exception(SWIG_TypeError, "Expected a non-empty arrayref");
    }
    av = (AV *)SvRV($input);

    /* allocate memory for $1 */
    len = av_len(av)+1; /* av_len(av) is like $#av */
    if (!len) {
	SWIG_exception(SWIG_TypeError, "Expected a non-empty arrayref");
    }
    $1 = g_new0(gchar *, len+1);

    for (i = 0; i < len; i++) {
	SV **sv = av_fetch(av, i, 0);
	g_assert(sv != NULL);
	$1[i] = g_strdup(SvPV_nolen(*sv));
    }

    /* final element is already NULL due to g_new0; xfer_filter_process takes
     * care of freeing this array, so we don't have to */
}

%newobject xfer_source_pattern;
XferElement *xfer_source_pattern(
    guint64 length,
    void * pattern,
    size_t pattern_length);

%newobject xfer_source_fd;
XferElement *xfer_source_fd(
    int fd);

%newobject xfer_source_directtcp_listen;
XferElement *xfer_source_directtcp_listen(void);

%inline %{
static DirectTCPAddr *
xfer_source_directtcp_listen_get_addrs(XferElement *elt) {
    return elt->input_listen_addrs;
}
%}

%newobject xfer_source_directtcp_connect;
XferElement *xfer_source_directtcp_connect(DirectTCPAddr *addrs);

%newobject xfer_filter_xor;
XferElement *xfer_filter_xor(
    unsigned char xor_key);

%newobject xfer_filter_process;
XferElement *xfer_filter_process(
    gchar **argv,
    gboolean need_root);
int get_err_fd(
    XferElement *elt);

%newobject xfer_dest_null;
XferElement *xfer_dest_null(
    guint32 prng_seed);

%newobject xfer_dest_buffer;
XferElement *xfer_dest_buffer(
    gsize max_size);

%cstring_output_allocate_size(gpointer *buf, gsize *size, );
void xfer_dest_buffer_get(
    XferElement *elt,
    gpointer *buf,
    gsize *size);

%newobject xfer_dest_fd;
XferElement *xfer_dest_fd(
    int fd);

%newobject xfer_dest_directtcp_listen;
XferElement *xfer_dest_directtcp_listen(void);

%inline %{
static DirectTCPAddr *
xfer_dest_directtcp_listen_get_addrs(XferElement *elt) {
    return elt->output_listen_addrs;
}
%}

%newobject xfer_dest_directtcp_connect;
XferElement *xfer_dest_directtcp_connect(DirectTCPAddr *addrs);

/*
 * Callback handling
 */

%types(amglue_Source *);
%{
static gboolean
xmsgsource_perl_callback(
    gpointer data,
    struct XMsg *msg,
    Xfer *xfer)
{
    dSP;
    amglue_Source *src = (amglue_Source *)data;
    SV *src_sv = NULL;
    SV *msg_sv = NULL;
    SV *xfer_sv = NULL;

    /* keep the source around long enough for the call to finish */
    amglue_source_ref(src);
    g_assert(src->callback_sv != NULL);

    ENTER;
    SAVETMPS;

    /* create a new SV pointing to 'src', and increase its refcount
     * accordingly. */
    amglue_source_ref(src);
    src_sv = SWIG_NewPointerObj(src, SWIGTYPE_p_amglue_Source,
				 SWIG_OWNER | SWIG_SHADOW);
    SvREFCNT_inc(src_sv);

    msg_sv = new_sv_for_xmsg(msg);
    xfer_sv = new_sv_for_xfer(xfer);

    PUSHMARK(SP);
    XPUSHs(sv_2mortal(src_sv));
    XPUSHs(sv_2mortal(msg_sv));
    XPUSHs(sv_2mortal(xfer_sv));
    PUTBACK;

    call_sv(src->callback_sv, G_EVAL|G_DISCARD);

    FREETMPS;
    LEAVE;

    /* we no longer need the src */
    amglue_source_unref(src);
    src = NULL;

    /* these may be gone, so NULL them out */
    src_sv = NULL;
    msg_sv = NULL;
    xfer_sv = NULL;

    /* check for an uncaught 'die'.  If we don't do this, then Perl will longjmp()
     * over the GMainLoop mechanics, leaving GMainLoop in an inconsistent (locked)
     * state. */
    if (SvTRUE(ERRSV)) {
	/* We handle this just the way the default 'die' handler in Amanda::Debug 
	 * does, but since Amanda's debug support may not yet be running, we back
	 * it up with an exit() */
	g_critical("%s", SvPV_nolen(ERRSV));
	exit(1);
    }

    return TRUE;
}
%}

%newobject xfer_get_amglue_source;
%inline %{
amglue_Source *
xfer_get_amglue_source(
    Xfer *xfer)
{
    return amglue_source_get(xfer_get_source(xfer),
	(GSourceFunc)xmsgsource_perl_callback);
}
%}

/*
 * XMsg and XMsgSource handling
 */

/*
 * The perl side
 */

/* First, a few macros to generate decent Perl */

%define PACKAGE(PKG)
%perlcode {
package PKG;
}
%enddef

%define XFER_ELEMENT_SUBCLASS_OF(PARENT)
%perlcode {
use vars qw(@ISA);
@ISA = qw( PARENT );
}
%enddef

%define XFER_ELEMENT_SUBCLASS()
XFER_ELEMENT_SUBCLASS_OF(Amanda::Xfer::Element)
%enddef

%define DECLARE_CONSTRUCTOR(C_CONSTRUCTOR)
%perlcode {
sub new { 
    my $pkg = shift;
    # The C function adds the proper blessing -- this function
    # just gets $pkg out of the way.
    C_CONSTRUCTOR(@_);
}
}
%enddef

%define OVERLOAD_REPR()
%perlcode {
use overload '""' => sub { $_[0]->repr(); };
# overload comparison, so users can ask if one obj == another
use overload '==' => sub {     Amanda::Xfer::same_elements($_[0], $_[1]); };
use overload '!=' => sub { not Amanda::Xfer::same_elements($_[0], $_[1]); };
}
%enddef

%define DECLARE_METHOD(METHOD_NAME, C_FUNCTION)
%perlcode {*METHOD_NAME = *C_FUNCTION;
}
%enddef

/* And now define the required perl classes */

PACKAGE(Amanda::Xfer::Xfer)
DECLARE_CONSTRUCTOR(Amanda::Xfer::xfer_new);
DECLARE_METHOD(DESTROY, Amanda::Xfer::xfer_unref);
OVERLOAD_REPR()
DECLARE_METHOD(repr, Amanda::Xfer::xfer_repr);
DECLARE_METHOD(get_status, Amanda::Xfer::xfer_get_status);
DECLARE_METHOD(get_source, Amanda::Xfer::xfer_get_amglue_source);
DECLARE_METHOD(start, Amanda::Xfer::xfer_start_with_callback);
DECLARE_METHOD(set_callback, Amanda::Xfer::xfer_set_callback);
DECLARE_METHOD(cancel, Amanda::Xfer::xfer_cancel);

/* ---- */

PACKAGE(Amanda::Xfer::Element)
DECLARE_METHOD(DESTROY, Amanda::Xfer::xfer_element_unref);
OVERLOAD_REPR()
DECLARE_METHOD(repr, Amanda::Xfer::xfer_element_repr);

/* ---- */

PACKAGE(Amanda::Xfer::Element::Glue)
XFER_ELEMENT_SUBCLASS()
/* no constructor -- internal use only */

/* ---- */

PACKAGE(Amanda::Xfer::Source::Fd)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::Xfer::xfer_source_fd)

/* ---- */

PACKAGE(Amanda::Xfer::Source::Random)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::Xfer::xfer_source_random)
DECLARE_METHOD(get_seed, Amanda::Xfer::xfer_source_random_get_seed)

/* ---- */

PACKAGE(Amanda::Xfer::Source::DirectTCPListen)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::Xfer::xfer_source_directtcp_listen)
DECLARE_METHOD(get_addrs, Amanda::Xfer::xfer_source_directtcp_listen_get_addrs)

/* ---- */

PACKAGE(Amanda::Xfer::Source::DirectTCPConnect)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::Xfer::xfer_source_directtcp_connect)

/* ---- */

PACKAGE(Amanda::Xfer::Source::Pattern)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::Xfer::xfer_source_pattern)

/* ---- */

PACKAGE(Amanda::Xfer::Filter::Xor)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::Xfer::xfer_filter_xor)

/* ---- */

PACKAGE(Amanda::Xfer::Filter::Process)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::Xfer::xfer_filter_process)
DECLARE_METHOD(get_stderr_fd, Amanda::Xfer::get_err_fd)

/* ---- */

PACKAGE(Amanda::Xfer::Dest::Fd)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::Xfer::xfer_dest_fd)

/* ---- */

PACKAGE(Amanda::Xfer::Dest::Null)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::Xfer::xfer_dest_null)

/* ---- */

PACKAGE(Amanda::Xfer::Dest::Buffer)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::Xfer::xfer_dest_buffer)
DECLARE_METHOD(get, Amanda::Xfer::xfer_dest_buffer_get)

/* ---- */

PACKAGE(Amanda::Xfer::Dest::DirectTCPListen)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::Xfer::xfer_dest_directtcp_listen)
DECLARE_METHOD(get_addrs, Amanda::Xfer::xfer_dest_directtcp_listen_get_addrs)

/* ---- */

PACKAGE(Amanda::Xfer::Dest::DirectTCPConnect)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(Amanda::Xfer::xfer_dest_directtcp_connect)

/* ---- */

PACKAGE(Amanda::Xfer::Msg)
%perlcode %{
use Data::Dumper;
use overload '""' => sub { $_[0]->repr(); };

sub repr {
    my ($self) = @_;
    local $Data::Dumper::Indent = 0;
    local $Data::Dumper::Terse = 1;
    local $Data::Dumper::Useqq = 1;

    my $typestr = Amanda::Xfer::xmsg_type_to_string($self->{'type'});
    my $str = "{ type => \$$typestr, elt => $self->{'elt'}, version => $self->{'version'},";

    my %skip = ( "type" => 1, "elt" => 1, "version" => 1 );
    for my $k (keys %$self) {
	next if $skip{$k};
	$str .= " $k => " . Dumper($self->{$k}) . ",";
    }

    # strip the trailing comma and add a closing brace
    $str =~ s/,$/ }/g;

    return $str;
}
%}

/* ---- */

PACKAGE(Amanda::Xfer)
%perlcode %{
# make Amanda::Xfer->new equivalent to Amanda::Xfer::Xfer->new (don't
# worry, the blessings work out just fine)
*new = *Amanda::Xfer::Xfer::new;

# try to load Amanda::XferServer, which is server-only.  If it's not found, then
# its classes just remain undefined.
BEGIN {
    use Amanda::Util;
    if (Amanda::Util::built_with_component("server")) {
	eval "use Amanda::XferServer;";
    }
}
%}
