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
#include "amanda.h"
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
	$to->remove();		    # dont' re-queue this timeout
	Amanda::MainLoop::quit();   # return from Amanda::MainLoop::run
    });

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
which point C<run()> returns.  You can check whether the loop is running with
C<Amanda::MainLoop::is_running()>.

In some cases, a sub should only run after the loop has started.  The
most common case is when a callback may call C<Amanda::MainLoop::quit>
immediately.  In this circumstance, use C<call_later>, which takes a
subref and an arbitrary number of arguments for that sub:

    my $cb = sub {
	my ($letters, $digits) = @_;
	# ..
	Amanda::MainLoop::quit();
    };
    Amanda::MainLoop::call_later($cb, "abc", 123);
    # ..
    Amanda::MainLoop::run();

Similarly, a tight loop of callbacks with no blocking operations can
lead to excessive stack consumption.  In this case, C<call_later()> is
useful, too. It queues the callback to run at the next cycle of the
MainLoop:

    sub might_delay {
        my ($cb) = @_;
        if (can_do_it_now()) {
            my $result = do_it();
            Amanda::MainLoop::call_later($cb, $result)
        } else {
            # ..
        }
    }

=head2 EVENT SOURCES

An event source is responsible for producing events.  A source may produce
multiple events over its lifetime.

The method C<$src->set_callback(\&cb)> sets the function that will be called
for a given source, and "attaches" the source to the main loop so that it will
begin generating events.  The arguments to the callback depend on the event
source, but the first argument is always the source itself.  Unless specified,
no other arguments are provided.

Event sources persist until they are removed with C<$src->remove()>, even if
the source itself is no longer accessible from Perl.  Although Glib supports
it, there is no provision for "automatically" removing an event source.  As an
example:

  sub start_timer { 
    my ($loops) = @_;

    Amanda::MainLoop::timeout_source(200)->set_callback(sub {
      my ($src) = @_;
      print "timer\n";
      if (--$loops <= 0) {
        $src->remove();
        Amanda::MainLoop::quit();
      }
    });
  }
  start_timer(10);
  Amanda::MainLoop::run();

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
dies before the source is created.  The callback is called with three
arguments: the event source, the PID, and the child's exit status.

Note that this source is totally incompatible with any thing that would cause
perl to change the SIGCHLD handler.  If SIGCHLD is changed, under some
circumstances the module will recognize this circumstance, add a warning to the
debug log, and continue operating.  However, it is impossible to catch all
possible situations.

=head3 File Descriptor

  my $src = Amanda::MainLoop::fd_source($fd, G_IO_IN);

This source will issuen an event whenever one of the given conditions is true
for the given file descriptor.  The conditions are from Glib's GIOCondition,
and are C<$G_IO_IN>, C<G_IO_OUT>, C<$G_IO_PRI>, C<$G_IO_ERR>, C<$G_IO_HUP>, and
C<$G_IO_NVAL>.  These constants are available with the import tag
C<:GIOCondition>.

Generally, when reading from a file descriptor, use C<$G_IO_IN|$G_IO_HUP> to
ensure that an EOF triggers an event as well.  Writing to a file descriptor can
simply use C<$G_IO_OUT>.

=head1 RELATION TO GLIB

Glib's main event loop is described in the Glib manual:
L<http://library.gnome.org/devel/glib/stable/glib-The-Main-Event-Loop.html>.
Note that Amanda depends only on the functionality available in Glib-2.2.0, so
many functions described in that document are not available in Amanda.  This
module provides a much-simplified interface to the glib library, and is not
intended as a generic wrapper for it.  Specifically:

=over

=item Amanda's perl-accessible main loop only runs a single C<GMainContext>, and
always runs in the main thread.

=item Besides idle sources, event priorities are not accessible from Perl.

=back

=head1 PROGRAMMING HINTS

Most often, callbacks are short, and can be specified as anonymous subs:

    $src->set_callback(sub {
	my ($src) = @_;
	# ...
    });

There is currently no means in place to specify extra arguments for a callback
when it is set.  If the callback needs access to other data, it should use a
Perl closure in the form of lexically scoped variables and a (possibly
anonymous) sub:

    {
	my $total_results = 0;

	$src->set_callback(sub {
	    my ($src, $result) = @_;
	    $total_results += $result;
	});
    }

Event sources are often set up in groups, e.g., a long-term operation and a
timeout.  When this is the case, be careful that all sources are removed when
the operation is complete.  The easiest way to accomplish this is to include all
sources in a lexical scope and remove them at the appropriate times:

    {
	my $op_src = long_operation_src();
	my $timeout_src = Amanda::MainLoop::timeout_source($timeout);

	sub finish {
	    $op_src->remove();
	    $timeout_src->remove();
	}

	$op_src->set_callback(sub {
	    print "Operation complete\n";
	    finish();
	});

	$timeout_src->set_callback(sub {
	    print "Operation timed out\n";
	    finish();
	});
    }

Both of these limitations may be lifted in future revisions of
L<Amanda::MainLoop>.

=cut
%}

/*
 * Looping
 */

%perlcode %{
# glib's g_is_main_loop_running() seems inaccurate, so we just
# track that information locally..
my $mainloop_running = 0;

my @waiting_to_call_later;
sub call_later {
    my ($sub, @args) = @_;

    # add the callback if nothing is waiting right now
    if (!@waiting_to_call_later) {
        timeout_source(0)->set_callback(sub {
            my ($src) = @_;
            $src->remove();

            while (@waiting_to_call_later) {
                my ($sub, @args) = @{pop @waiting_to_call_later};
                $sub->(@args);
            }
        });
    }

    push @waiting_to_call_later, [ $sub, @args ];
}

sub run {
    $mainloop_running = 1;
    run_c();
    $mainloop_running = 0;
}

sub is_running {
    return $mainloop_running;
}
%}

%inline %{
void run_c(void) {
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

%{ static void amglue_source_remove(amglue_Source *self); %}

%rename(Source) amglue_Source;
typedef struct amglue_Source {
    %extend {
	/* Constructor: use one of the package-level functions, below */
	amglue_Source() {
	    die("Amanda::MainLoop::Source is an abstract base class");
	}

	/* Destructor: just unref the object */
	~amglue_Source() {
	    amglue_source_unref(self);
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

		/* the link from the GSource to the amglue_Source is
		 * now in use, so we increment the amglue_Source's 
		 * refcount. */
		amglue_source_ref(self);
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
	    amglue_source_remove(self);
	}
    }
} amglue_Source;

%{
/* Detach a source from the mainloop and remove it from play.  This is broken
 * out as a separate function because it's also used from some callbacks */
static void
amglue_source_remove(
    amglue_Source *self)
{
    /* protect against self being freed out from under us */
    amglue_source_ref(self);

    if (self->state == AMGLUE_SOURCE_ATTACHED) {
	/* unref any perl callback */
	if (self->callback_sv) {
	    SvREFCNT_dec(self->callback_sv);
	    self->callback_sv = NULL;
	}

	/* undo the ref made in set_callback() */
	amglue_source_unref(self);

	g_source_destroy(self->src);
    }

    self->state = AMGLUE_SOURCE_DESTROYED;

    /* reverse the "protection" increment used above */
    amglue_source_unref(self);
}
%}

/* "Generic" callback function for a GSource that actually uses the GSourceFunc
 * prototype.  The source of this function also serves as a prototype for other,
 * more advanced callbacks.  Due to perl's heavy use of precompiler macros, it's
 * not possible to break this down any further. */
%{
static gboolean
amglue_source_callback_simple(
    gpointer *data)
{
    dSP;
    amglue_Source *src = (amglue_Source *)data;
    SV *src_sv = NULL;

    g_assert(src->callback_sv != NULL);

    ENTER;
    SAVETMPS;

    /* create a new SV pointing to 'src', and increase our refcount
     * accordingly.  The SV is mortal, so FREETMPS will decrease the 
     * refcount, unless the callee keeps a copy of it somewhere */
    amglue_source_ref(src);
    src_sv = SWIG_NewPointerObj(src, SWIGTYPE_p_amglue_Source,
				 SWIG_OWNER | SWIG_SHADOW);

    PUSHMARK(SP);
    XPUSHs(src_sv);
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

/* Constructors for some general-purpose sources */

/* timeout source */
%newobject timeout_source;
%inline %{
amglue_Source *
timeout_source(
    guint interval)
{
    return amglue_source_new(g_timeout_source_new(interval), 
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
    return amglue_source_new(idle_source,
	(GSourceFunc)amglue_source_callback_simple);
}
%}

/* child watch source */
%{
static gboolean
child_watch_source_callback(
    pid_t pid,
    gint status,
    gpointer data)
{
    dSP;
    amglue_Source *src = (amglue_Source *)data;
    SV *src_sv;

    g_assert(src->callback_sv != NULL);

    ENTER;
    SAVETMPS;

    /* create a new SV pointing to 'src', and increase our refcount
     * accordingly.  The SV is mortal, so FREETMPS will decrease the 
     * refcount, unless the callee keeps a copy of it somewhere */
    amglue_source_ref(src);
    src_sv = SWIG_NewPointerObj(src, SWIGTYPE_p_amglue_Source,
				 SWIG_OWNER | SWIG_SHADOW);

    PUSHMARK(SP);
    XPUSHs(src_sv);
    XPUSHs(sv_2mortal(newSViv(pid)));
    XPUSHs(sv_2mortal(newSViv(status)));
    PUTBACK;

    call_sv(src->callback_sv, G_EVAL|G_DISCARD);

    /* child watch sources automatically destroy themselves after the
     * child dies, so we mark the amglue_Source as destroyed, too. */
    amglue_source_remove(src);

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
%newobject child_watch_source;
%inline %{
amglue_Source *
child_watch_source(
    gint pid)
{
    GSource *child_watch_source = new_child_watch_source(pid);
    return amglue_source_new(child_watch_source,
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
    return amglue_source_new(fdsource,
	(GSourceFunc)amglue_source_callback_simple);
}
%}
