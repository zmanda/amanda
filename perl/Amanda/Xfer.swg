/*
 * Copyright (c) Zmanda, Inc.  All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1
 * as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
 *
 * Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 */

%module "Amanda::Xfer"
%include "amglue/amglue.swg"
%include "exception.i"
%import "Amanda/MainLoop.swg"
%import "Amanda/Device.swg"

%{
#include "glib-util.h"
#include "amxfer.h"
%}

%perlcode %{
=head1 NAME

Amanda::Xfer - the transfer architecture

=head1 SYNOPSIS

  use Amanda::MainLoop;
  use Amanda::Xfer qw( :constants );
  use POSIX;

  my $infd = POSIX::open("input", POSIX::O_RDONLY, 0);
  my $outfd = POSIX::open("output", POSIX::O_CREAT|POSIX::O_WRONLY, 0640);
  my $xfer = Amanda::Xfer->new([
    Amanda::Xfer::Source::Fd->new($infd),
    Amanda::Xfer::Dest::Fd->new($outfd)
  ]);
  $xfer->get_source()->set_callback(sub {
      my ($src, $xmsg, $xfer) = @_;
      print "Message from $xfer: $xmsg\n"; # use stringify operations
      if ($xfer->get_status() == $XFER_DONE) {
	  $src->remove();
	  Amanda::MainLoop::quit();
      }
  });
  $xfer->start();
  Amanda::MainLoop::run();
  
See L<http://wiki.zmanda.com/index.php/XFA> for background on the transfer
architecture.

=head1 API STATUS

Fluid.

=head1 Amanda::Xfer Objects

A new transfer is created with C<< Amanda::Xfer->new() >>, which takes an arrayref
giving the transfer elements which should compose the transfer.

The resulting object has the following methods:

=over

=item get_source()

Get the L<Amanda::MainLoop> event source through which messages will be
delivered for this transfer.  Use its C<set_callback> method to connect a perl
sub for processing events.  You I<must> C<release> the source when the
transfer is complete!

The callback from this event source receives three arguments: the event source,
the message, and a reference to the controlling transfer.  See the description of
C<Amanda::Xfer::Msg>, below, for details.

=item start()

Start this transfer.  Processing takes place asynchronously, and messages will
begin queueing up immediately.

=item cancel()

Stop transferring data.  The transfer will send an C<XMSG_CANCEL>, "drain" any
buffered data as best it can, and then complete normally with an C<XMSG_DONE>.

=item get_status()

Get the transfer's status.  The result will be one of C<$XFER_INIT>,
C<$XFER_START>, C<$XFER_RUNNING>, or C<$XFER_DONE>.  These symbols are
available for import with the tag C<:constants>.

=item repr()

Return a string representation of this transfer, suitable for use in debugging
messages.  This method is automatically invoked when a transfer is interpolated
into a string:
  print "Starting $xfer\n";

=back

=head1 Amanda::Xfer::Element objects

The individual transfer elements that compose a transfer are instances of
subclasses of Amanda::Xfer::Element.  All such objects have a C<repr()> method,
similar to that for transfers, and support a similar kind of string
interpolation.

Note that the names of these classes contain the words "Source", "Filter", and
"Dest".  This is merely suggestive of their intended purpose -- there are no
such abstract classes.

=head2 Transfer Sources

=head3 Amanda::Xfer::Source::Device

  Amanda::Xfer::Source::Device->new($device);

This source reads data from a device.  The device should already be queued up
for reading (C<$device->seek_file(..)>).  The element will read until the end
of the device file.

=head3 Amanda::Xfer::Source::Fd

  Amanda::Xfer::Source::Fd->new(fileno($fh));

This source reads data from a file descriptor.  It reads until EOF, but does
not close the descriptor.  Be careful not to let Perl close the file for you!

=head3 Amanda::Xfer::Source::Random

  Amanda::Xfer::Source::Random->new($length, $seed);

This source provides I<length> bytes of random data (or an unlimited amount
of data if I<length> is zero).  C<$seed> is the seed used
to generate the random numbers; this seed can be used in a destination to
check for correct output.

=head3 Amanda::Xfer::Source::Pattern

  Amanda::Xfer::Source::Pattern->new($length, $pattern);

This source provides I<length> bytes containing copies of
I<pattern>. If I<length> is zero, the source provides an unlimited
number of bytes.

=head2 Transfer Filters

=head3 Amanda::Xfer::Filter:Xor

  Amanda::Xfer::Filter::Xor->new($key);

This filter applies a bytewise XOR operation to the data flowing through it.

=head2 Transfer Destinations

=head3 Amanda::Xfer::Dest::Device

  Amanda::Xfer::Dest::Device->new($device, $max_memory);

This source writes data to a device.  The device should already be queued up
for writing (C<$device->start_file(..)>).  No more than C<$max_memory> will be
used for buffers.  Use zero for the default buffer size.  On completion of the
transfer, the file will be finished.

=head3 Amanda::Xfer::Dest::Fd

  Amanda::Xfer::Dest::Fd->new(fileno($fh));

This destination writes data to a file descriptor.  The file is not closed
after the transfer is completed.  Be careful not to let Perl close the file
for you!

=head3 Amanda::Xfer::Dest::Null

  Amanda::Xfer::Dest::Null->new($seed);

This destination discards the data it receives.  If C<$seed> is nonzero, then
the element will validate that it receives the data that
C<Amanda::Xfer::Source::Random> produced with the same seed.  No validation is
performed if C<$seed> is zero.

=head1 Amanda::Xfer::Msg objects

Messages are simple hashrefs, with a few convenience methods.  Like transfers,
they have a C<repr()> method that formats the message nicely, and is available
through string interpolation:
  print "Received message $msg\n";

Every message has the following keys:

=over

=item type

The message type -- one of the C<xmsg_type> constants available from the import
tag C<:constants>.

=item elt

The transfer element that sent the message.

=item version

The version of the message.  This is used to support extensibility of the protocol.

=back

The canonical description of the message types and keys is in C<xfer-src/xmsg.h>, and is
not duplicated here.

=cut
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
amglue_copy_to_tag(xmsg_type, constants);

/*
 * Wrapping machinery
 */

%{
/* Return a new SV with refcount 1 representing the given C object
 * with the given class.
 *
 * @param c_obj: the object to represent
 * @param perl_class: the perl with which to bless and tie the SV
 */
static SV *
new_sv_for_c_obj(
    gpointer c_obj,
    const char *perl_class)
{
    SV *sv = newSV(0);

    /* Make an SV that contains a pointer to the object, and bless it
     * with the appropriate class. */
    sv_setref_pv(sv, perl_class, c_obj);

    return sv;
}

/* Return a new SV representing a transfer.
 *
 * @param xfer: the transfer to represent
 */
static SV *
new_sv_for_xfer(
    Xfer *xfer)
{
    if (!xfer) return &PL_sv_undef;

    xfer_ref(xfer);
    return new_sv_for_c_obj(xfer, "Amanda::Xfer::Xfer");
}

/* Return a new SV representing a transfer element.
 *
 * @param xe: the transfer element to represent
 */
static SV *
new_sv_for_xfer_element(
    XferElement *xe)
{
    const char *perl_class;

    if (!xe) return &PL_sv_undef;

    perl_class = XFER_ELEMENT_GET_CLASS(xe)->perl_class;
    if (!perl_class) die("Attempt to wrap an XferElementClass with no perl class!");
    g_object_ref(xe);
    return new_sv_for_c_obj(xe, perl_class);
}

/* Return the C object buried in an SV, asserting that the perl SV is
 * derived from derived_from.  Returns NULL for undefined perl values.
 *
 * This function is based on SWIG's SWIG_Perl_ConvertPtr.  The INT2PTR
 * situation certainly looks strange, but is documented in perlxs.
 *
 * @param sv: the SV to convert
 * @param derived_from: perl class from which the SV should be derived
 * @return: underlying pointer
 */
static gpointer
c_obj_from_sv(
    SV *sv,
    const char *derived_from)
{
    SV *referent;
    IV tmp;

    if (!sv) return NULL;
    if (!SvOK(sv)) return NULL;

    /* Peel back the layers.  The sv should be a blessed reference to a PV,
     * and we check the class against derived_from to ensure we have the right
     * stuff. */
    if (!sv_isobject(sv) || !sv_derived_from(sv, derived_from)) {
	croak("Value is not an object of type %s", derived_from);
	return NULL;
    }

    referent = (SV *)SvRV(sv);
    tmp = SvIV(referent);
    return INT2PTR(gpointer, tmp);
}

/* Convert an SV to an Xfer.  The Xfer's reference count is not
 * incremented -- this is a "borrowed" reference.
 *
 * @param sv: the perl value
 * @returns: pointer to the corresponding transfer, or NULL
 */
static Xfer *
xfer_from_sv(
    SV *sv)
{
    return (Xfer *)c_obj_from_sv(sv, "Amanda::Xfer::Xfer");
}

/* Convert an SV to an XferElement.  The element's reference count is
 * not incremented -- this is a "borrowed" reference.
 *
 * @param sv: the perl value
 * @returns: pointer to the corresponding transfer element, or NULL.
 */
static XferElement *
xfer_element_from_sv(
    SV *sv)
{
    return (XferElement *)c_obj_from_sv(sv, "Amanda::Xfer::Element");
}

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
void xfer_start(Xfer *xfer);
void xfer_cancel(Xfer *xfer);
/* xfer_get_source is implemented below */

%inline %{
/* SWIG wants to treat this as a function */
#define xfer_get_status(xfer) ((xfer)->status)
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
/* xfer_element_start -- private */
/* xfer_element_cancel -- private */

/* subclass constructors */

/* N.B. When adding new classes, ensure that the class_init function
 * sets perl_class to the appropriate value. */

%newobject xfer_source_device;
XferElement *xfer_source_device(
    Device *device);

%newobject xfer_source_random;
XferElement *xfer_source_random(
    guint64 length,
    guint32 seed);

%typemap(in) (void * pattern, size_t pattern_length) {
 size_t len;
 char * pat;

 pat = SvPV($input, len);
 $1 = g_memdup(pat, len);
 $2 = len;
}

%newobject xfer_source_random;
XferElement *xfer_source_pattern(
    guint64 length,
    void * pattern,
    size_t pattern_length);

%newobject xfer_source_fd;
XferElement *xfer_source_fd(
    int fd);

%newobject xfer_filter_xor;
XferElement *xfer_filter_xor(
    unsigned char xor_key);

%newobject xfer_dest_device;
XferElement *xfer_dest_device(
    Device *device,
    size_t max_memory);

%newobject xfer_dest_null;
XferElement *xfer_dest_null(
    guint32 prng_seed);

%newobject xfer_dest_fd;
XferElement *xfer_dest_fd(
    int fd);

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

    g_assert(src->callback_sv != NULL);

    ENTER;
    SAVETMPS;

    /* create a new SV pointing to 'src', and increase its refcount
     * accordingly.  The SV is mortal, so FREETMPS will decrease the 
     * refcount, unless the callee keeps a copy of it somewhere */
    amglue_source_ref(src);
    src_sv = SWIG_NewPointerObj(src, SWIGTYPE_p_amglue_Source,
				 SWIG_OWNER | SWIG_SHADOW);

    PUSHMARK(SP);
    XPUSHs(src_sv);
    XPUSHs(sv_2mortal(new_sv_for_xmsg(msg)));
    XPUSHs(sv_2mortal(new_sv_for_xfer(xfer)));
    PUTBACK;

    call_sv(src->callback_sv, G_EVAL|G_DISCARD);

    FREETMPS;
    LEAVE;

    /* these may have been freed, so don't use them after this point */
    src_sv = NULL;
    src = NULL;

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

%define XFER_ELEMENT_SUBCLASS()
%perlcode {
use vars qw(@ISA);
@ISA = qw( Amanda::Xfer::Element );
}
%enddef

%define DECLARE_CONSTRUCTOR(C_CONSTRUCTOR)
%perlcode {
sub new { 
    my $pkg = shift;
    # The C function adds the proper blessing -- this function
    # just gets $pkg out of the way.
    Amanda::Xfer::C_CONSTRUCTOR(@_);
}
}
%enddef

%define OVERLOAD_REPR()
%perlcode {use overload '""' => sub { $_[0]->repr(); };
}
%enddef

%define DECLARE_METHOD(METHOD_NAME, C_FUNCTION)
%perlcode {*METHOD_NAME = *Amanda::Xfer::C_FUNCTION;
}
%enddef

/* And now define the required perl classes */

PACKAGE(Amanda::Xfer::Xfer)
DECLARE_CONSTRUCTOR(xfer_new);
DECLARE_METHOD(DESTROY, xfer_unref);
OVERLOAD_REPR()
DECLARE_METHOD(repr, xfer_repr);
DECLARE_METHOD(get_status, xfer_get_status);
DECLARE_METHOD(get_source, xfer_get_amglue_source);
DECLARE_METHOD(start, xfer_start);
DECLARE_METHOD(cancel, xfer_cancel);

/* ---- */

PACKAGE(Amanda::Xfer::Element)
DECLARE_METHOD(DESTROY, xfer_element_unref);
OVERLOAD_REPR()
DECLARE_METHOD(repr, xfer_element_repr);

/* ---- */

PACKAGE(Amanda::Xfer::Element::Glue)
XFER_ELEMENT_SUBCLASS()
/* no constructor -- internal use only */

/* ---- */

PACKAGE(Amanda::Xfer::Source::Device)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(xfer_source_device)

/* ---- */

PACKAGE(Amanda::Xfer::Source::Fd)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(xfer_source_fd)

/* ---- */

PACKAGE(Amanda::Xfer::Source::Random)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(xfer_source_random)

/* ---- */

PACKAGE(Amanda::Xfer::Source::Pattern)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(xfer_source_pattern)

/* ---- */

PACKAGE(Amanda::Xfer::Filter::Xor)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(xfer_filter_xor)

/* ---- */

PACKAGE(Amanda::Xfer::Dest::Device)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(xfer_dest_device)

/* ---- */

PACKAGE(Amanda::Xfer::Dest::Fd)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(xfer_dest_fd)

/* ---- */

PACKAGE(Amanda::Xfer::Dest::Null)
XFER_ELEMENT_SUBCLASS()
DECLARE_CONSTRUCTOR(xfer_dest_null)

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
%}
