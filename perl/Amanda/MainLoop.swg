/*
 * Copyright (c) 2008 Zmanda, Inc.  All Rights Reserved.
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

%module "Amanda::MainLoop"
%include "amglue/amglue.swg"
%include "exception.i"

%{
#include "event.h"
%}

%perlcode %{
=head1 NAME

Amanda::MainLoop - Perl interface to the Glib MainLoop

=head1 SYNOPSIS

    use Amanda::MainLoop;
    
    my $to = Amanda::MainLoop::timeout_source(2000);
    $to->set_callback(sub { 
	print "Time's Up!\n";
	Amanda::MainLoop::destroy_source($to);
	Amanda::MainLoop::quit();
    });
    $to->attach();

    Amanda::MainLoop::run();

Note that all functions in this module are individually available for export,
e.g.,

    use Amanda::MainLoop qw(run quit);

=head1 OVERVIEW

The main event loop of an application is a tight loop which waits for events,
and calls functions to respond to those events.  This design allows an IO-bound
application to multitask within a single thread, by responding to IO events as
they occur instead of blocking on particular IO operations.

The Amanda security API, transfer API, and other components rely on the event
loop to allow them to respond to their own events in a timely fashion.

The overall structure of an application, then, is to initialize its state,
register callbacks for some events, and begin looping.  When some
application-defined state is reached, the loop is terminated and the
application cleans up and exits.

=head2 LOOPING

The actual event loop takes place within a call to C<Amanda::MainLoop::run()>.
This function executes until a call to C<Amanda::MainLoop::quit()> occurs, at
which point C<run()> returns.

=head2 EVENT SOURCES

An event source is responsible for producing events.  A source may produce
multiple events over its lifetime.

The method C<$src->set_callback(\&cb)> sets the function that will be called
for a given source, and "attaches" the source to the main loop so that it will
begin generating events.  The arguments to the callback depend on the event
source.  Unless specified, no arguments are provided.

Event sources persist until they are removed with C<$src->remove()>.  Although
Glib supports it, there is no provision for "automatically" removing an event
source.

=head3 Timeout

  my $src = Amanda::MainLoop::timeout_source(10000);

A timeout source will create events at the specified interval, specified in
milliseconds (thousandths of a second).  The events will continue until the
source is destroyed.

=head3 Idle

  my $src = Amanda::MainLoop::idle_source(2);

An idle source will create events continuously except when a higher-priority
source is emitting events.  Priorities are generally small positive integers,
with larger integers denoting lower priorities.  The events will continue until
the source is destroyed.

=head3 Child Watch

  my $src = Amanda::MainLoop::child_watch_source($pid);

A child watch source will issue an event when the process with the given PID
dies.  To avoid race conditions, it will issue an event even if the process
dies before the source is created.  The callback is called with two arguments:
the PID and the child's exit status.

Note that this source is totally incompatible with any thing that would cause
perl to change the SIGCHLD handler.  In particular, C<open .. "|-"> cannot be
used in the same process as this source.

=head3 File Descriptor

  my $src = Amanda::MainLoop::fd_source($fd, G_IO_IN);

This source will issuen an event whenever one of the given conditions is true
for the given file descriptor.  The conditions are from Glib's GIOCondition,
and are C<$G_IO_IN>, C<G_IO_OUT>, C<$G_IO_PRI>, C<$G_IO_ERR>, C<$G_IO_HUP>, and
C<$G_IO_NVAL>.  These constants are available with the import tag
C<:GIOCondition>.

=head1 RELATION TO GLIB

Glib's main event loop is described in the Glib manual:
L<http://library.gnome.org/devel/glib/stable/glib-The-Main-Event-Loop.html>.
Note that Amanda depends only on the functionality available in Glib-2.2.0, so
many functions described in that document are not available in Amanda.  This
module provides a much-simplified interface to the glib library, and is not
intended as a generally-useful wrapper for it.

Amanda's perl-accessible main loop only runs a single C<GMainContext>, and
always runs in the main thread.

=cut
%}

/*
 * Looping
 */

%inline %{
void run(void) {
    g_main_loop_run(default_main_loop());
}

void quit(void) {
    g_main_loop_quit(default_main_loop());
}
%}

/*
 * Event Sources
 */

/* First we wrap the amglue_Source struct, defined in
 * perl/amglue/mainloop.h, into a Perl object (named
 * Amanda::MainLoop::Source).  After that appear several 
 * constructors for various event sources.
 */

%rename(Source) amglue_Source;
typedef struct amglue_Source {
    %extend {
	/* Constructor: use one of the package-level functions, below */
	amglue_Source() {
	    die("Cannot create event sources directly");
	}

	/* Destructor: when Perl is no longer interested in this object,
	 * then detach from the main loop and clean up. */
	~amglue_Source() {
	    amglue_source_free(self);
	}

	/* a "cheater's typemap" to just pass the SV along */
	%typemap(in) SV *callback_sub "$1 = $input;"
	void set_callback(SV *callback_sub) {
	    /* Attach the source to the default mainloop context, so
	     * that it will start generating events.  If it's already
	     * been destroyed, then bail with a fatal error.
	     */
	    if (self->state == AMGLUE_SOURCE_DESTROYED) {
		die("This source has already been removed");
	    } else if (self->state == AMGLUE_SOURCE_NEW) {
		self->state = AMGLUE_SOURCE_ATTACHED;
		g_source_attach(self->src, NULL);
		g_source_unref(self->src);
	    }

	    /* whoever created this Source object conveniently left
	     * the proper C-side callback for us.  This function has
	     * the appropriate calling signature for this GSource, and
	     * knows how to reflect that into Perl.  It expects the SV to
	     * be provided as its 'data' argument.  'perlcall' suggests
	     * that we make a copy of this SV, in case the user later
	     * modifies it. */
	    if (self->callback_sv) {
		SvSetSV(self->callback_sv, callback_sub);
	    } else {
		self->callback_sv = newSVsv(callback_sub);
		g_source_set_callback(self->src, self->callback,
		    (gpointer)self, NULL);
	    }
	}
	/* delete the cheater's typemap */
	%typemap(in) SV *sv;

	void remove(void) {
	    if (self->state == AMGLUE_SOURCE_ATTACHED) {
		self->state = AMGLUE_SOURCE_DESTROYED;
		g_source_destroy(self->src);
	    }
	}
    }
} amglue_Source;

/* Constructors for some general-purpose sources */

/* timeout source */
%newobject timeout_source;
%inline %{
amglue_Source *
timeout_source(
    guint interval)
{
    return amglue_new_source(g_timeout_source_new(interval), 
	(GSourceFunc)amglue_source_callback_simple);
}
%}

/* idle source */
%newobject idle_source;
%inline %{
amglue_Source *
idle_source(
    gint priority)
{
    GSource *idle_source = g_idle_source_new();
    g_source_set_priority(idle_source, priority);
    return amglue_new_source(idle_source,
	(GSourceFunc)amglue_source_callback_simple);
}
%}

/* child watch source */
#if sizeof(gint) < sizeof(pid_t)
#error 'gint' type is too small to hold a PID
#endif

%{
static gboolean
child_watch_source_callback(
    pid_t pid,
    gint status,
    gpointer data)
{
    dSP;
    amglue_Source *src = (SV *)data;
    g_assert(src->callback_sv != NULL);

    /* push some arguments */
    PUSHMARK(SP);
    XPUSHs(sv_2mortal(newSViv(pid)));
    XPUSHs(sv_2mortal(newSViv(status)));
    PUTBACK;

    /* child watch sources automatically destroy themselves after the
     * child dies, so we mark the amglue_Source as destroyed, too.  We
     * do this now, in case src is destroyed later. */
    src->state = AMGLUE_SOURCE_DESTROYED;

    call_sv(src->callback_sv, G_EVAL|G_DISCARD);

    /* 'src' may have been freed at this point! */
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
%newobject child_watch_source;
%inline %{
amglue_Source *
child_watch_source(
    gint pid)
{
    GSource *child_watch_source = new_child_watch_source(pid);
    return amglue_new_source(child_watch_source,
	(GSourceFunc)child_watch_source_callback);
}
%}

/* fd source */
%apply gint { GIOCondition };
amglue_add_flag_tag_fns(GIOCondition);
amglue_add_constant(G_IO_IN, GIOCondition);
amglue_add_constant(G_IO_OUT, GIOCondition);
amglue_add_constant(G_IO_PRI, GIOCondition);
amglue_add_constant(G_IO_ERR, GIOCondition);
amglue_add_constant(G_IO_HUP, GIOCondition);
amglue_add_constant(G_IO_NVAL, GIOCondition);
amglue_copy_to_tag(GIOCondition, constants);

%newobject fd_source;
%inline %{
amglue_Source *
fd_source(
    gint fd,
    GIOCondition events)
{
    GSource *fdsource = new_fdsource(fd, events);
    return amglue_new_source(fdsource,
	(GSourceFunc)amglue_source_callback_simple);
}
%}
