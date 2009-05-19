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
register callbacks for some events, and begin looping.  In each iteration, the
loop waits for interesting events to occur (data available for reading or
writing, timeouts, etc.), and then calls functions to handle those interesting
things.  Thus, the application spends most of its time waiting.  When some
application-defined state is reached, the loop is terminated and the
application cleans up and exits.

The Glib main loop takes place within a call to C<Amanda::MainLoop::run()>.
This function executes until a call to C<Amanda::MainLoop::quit()> occurs, at
which point C<run()> returns.  You can check whether the loop is running with
C<Amanda::MainLoop::is_running()>.

=head1 HIGH-LEVEL INTERFACE

The functions in this section are intended to make asynchronous programming as
simple as possible.  They are implemented on top of the interfaces described in
the LOW-LEVEL INTERFACE section.

=head3 call_later

In most cases, a callback does not need to be invoked immediately.  In fact,
because Perl does not do tail-call optimization, a long chain of callbacks may
cause the perl stack to grow unnecessarily.

The solution is to queue the callback for execution on the next iteration of
the main loop, and C<call_later($cb, @args)> does exactly this.

    sub might_delay {
        my ($cb) = @_;
        if (can_do_it_now()) {
            my $result = do_it();
            Amanda::MainLoop::call_later($cb, $result)
        } else {
            # ..
        }
    }

When starting the main loop, an application usually has a sub that should run
after the loop has started.  C<call_later> works in this situation, too.

    my $main = sub {
	# ..
	Amanda::MainLoop::quit();
    };
    Amanda::MainLoop::call_later($main);
    # ..
    Amanda::MainLoop::run();

=head3 make_cb

As an optimization, C<make_cb> wraps a sub with a call to call_later while also
naming the sub (using C<Sub::Name>, if available):

    my $fetched_cb = make_cb(fetched_cb => sub {
	# .. callback body
    }

In general, C<make_cb> should be used whenever a callback is passed to some
other library.  For example, the Changer API (see L<Amanda::Changer>) might
be invoked like this:

    my $reset_finished_cb = make_cb(reset_finished_cb => sub {
	my ($err) = @_;
	die "while resetting: $err" if $err;
	# ..
    });

Be careful I<not> to use C<make_cb> in cases where some action must take place
before the next iteration of the main loop.  In practice, this means C<make_cb>
should be avoided with file-descriptor callbacks, which will trigger repeatedly
until the descriptors' needs are addressed.

C<make_cb> is exported automatically.

=head3 call_after

Sometimes you need the MainLoop equivalent of C<sleep()>.  That comes in the
form of C<call_later($delay, $cb, @args)>, which takes a delay (in
milliseconds), a sub, and an arbitrary number of arguments.  The sub is called
with the arguments after the delay has elapsed.

    sub countdown {
	my $counter;
	$counter = sub {
	    print "$i..\n";
	    if ($i) {
		Amanda::MainLoop::call_after(1000, $counter, $i-1);
	    }
	}
	$counter->(10);
    }

The function returns the underlying event source (see below), enabling the
caller to cancel the pending call:

    my $tosrc = Amanda::MainLoop::call_after(15000, $timeout_cb):
    # ...data arrives before timeout...
    $tosrc->remove();

=head3 call_on_child_termination

To monitor a child process for termination, give its pid to
C<call_on_child_termination($pid, $cb, @args)>.  When the child exits for any
reason, this will collect its exit status (via C<waitpid>) and call C<$cb> as

    $cb->($exitstatus, @args);

Like C<call_after>, this function returns the event source to allow early
cancellation if desired.

=head3 async_read

    async_read(
	fd => $fd,
	size => $size,        # optional, default 0
	async_read_cb => $async_read_cb,
	args => [ .. ]);      # optional

This function will read C<$size> bytes when they are available from file
descriptor C<$fd>, and invoke the callback with the results:

    $async_read_cb->($err, $buf, @args);

If C<$size> is zero, then the callback will get whatever data is available as
soon as it is available, up to an arbitrary buffer size.  If C<$size> is
nonzero, then a short read may still occur if C<$size> bytes do not become
available simultaneously.  On EOF, C<$buf> will be the empty string.  It is the
caller's responsibility to set C<$fd> to non-blocking mode.  Note that not all
operating sytems generate errors that might be reported here.  For example, on
Solaris an invalid file descriptor will be silently ignored.

The return value is an event source, and calling its C<remove> method will
cancel the read.  It is an error to have more than one C<async_read> operation
on a single file descriptor at any time, and will lead to unpredictable
results.

This function adds a new FdSource every time it is invoked, so it is not
well-suited to processing large amounts of data.  For that purpose, consider
using the low-level interface or, better, the transfer architecture (see
L<Amanda:Xfer>).

=head3 async_write

    async_write(
	fd => $fd,
	data => $data,
	async_write_cb => $async_write_cb,
	args => [ .. ]);      # optional

This function will write C<$data> to file descriptor C<$fd> and invoke the
callback with the number of bytes written:

    $cb->($err, $bytes_written, @args);

If C<$bytes_written> is less than then length of <$data>, then an error
occurred, and is given in C<$err>.  As for C<async_read>, the caller should set
C<$fd> to non-blocking mode.  Multiple parallel invocations of this function
for the same file descriptor are allowed and will be serialized in the order
the calls were made:

    async_write($fd, "HELLO!\n",
	async_write_cb => make_cb(wrote_hello => sub {
	    print "wrote 'HELLO!'\n";
	}));
    async_write($fd, "GOODBYE!\n",
	async_write_cb => make_cb(wrote_goodbye => sub {
	    print "wrote 'GOODBYE!'\n";
	}));

In this case, the two strings are guaranteed to be written in the same order,
and the callbacks will be called in the correct order.

Like async_read, this function may add a new FdSource every time it is invoked,
so it is not well-suited to processing large amounts of data.

=head1 ASYNCHRONOUS STYLE

=head2 USING CALLBACKS

Most often, callbacks are short, and can be specified as anonymous subs.  They
should be specified with make_cb, like this:

    some_async_function(make_cb(foo_cb => sub {
	my ($x, $y) = @_;
	# ...
    }));

If a callback is more than about two lines, specify it in a named variable,
rather than directly in the function call:

    my $foo_cb = make_cb(foo_cb => sub {
	my ($src) = @_;
	# .
	# .  long function
	# .
    });
    some_async_function($foo_cb);

When using callbacks from an object-oriented package, it is often useful to
treat a method as a callback.  This requires an anonymous sub "wrapper", which
can be written on one line:

    some_async_function(sub { $self->foo_cb(@_) });

Some operations require a long chain of asynchronous operations.  For example,
if the results of one operation are required to initiate another.  It's easy
for such code to become virtually unreadable, with execution jumping from line
to line.  Using a C<%subs> hash can help make the code more linear - executing
from the top to the bottom.  The idea is to place each callback into the hash
under a short but descriptive key, and then chain the callbacks together using
the values from that hash.

    sub send_file {
	my ($hostname, $port, $data, $sendfile_cb) = @_;
	my ($addr, $socket); # shared lexical variables
	my %subs;
	$subs{'lookup_addr'} = make_cb(lookup_addr => sub {
	    return async_gethostbyname(hostname => $hostname, ghbn_cb => $subs{'got_addr'});
	});
	$subs{'ghbn_cb'} = make_cb(ghbn_cb => sub {
	    my ($err, $hostinfo) = @_;
	    die $err if $err;
	    $addr = $hostinfo->{'ipaddr'};
	    return $subs{'connect'}->();
	});
	$subs{'connect'} = make_cb(connect => sub {
	    return async_connect(
		ipaddr => $addr,
		port => $port,
		connect_cb => $subs{'connect_cb'},
	    );
	});
	$subs{'connect_cb'} = make_cb(connect_cb => sub {
	    my ($err, $conn_sock) = @_;
	    die $err if $err;
	    $socket = $conn_sock;
	    return $subs{'write_block'}->();
	});
	# ...
	$subs{'lookup_addr'}->();
    }

There are more anonymous subs here than are strictly necessary: the body of
C<connect> could be appended to C<ghbn_cb>.  The extra subs make the overall
operation more readable by adding "punctuation" to separate the task of
handling a callback (C<ghbn_cb>) from starting the next operation (C<connect>).

Also note that the enclosing scope contains some lexical (C<my>) variables
which are shared by several of the callbacks.

=head2 WRITING ASYNCHRONOUS INTERFACES

When designing a library or interface that will accept and invoke callbacks,
follow these guidelines so that users of the interface will not need to
remember special rules.

Each callback signature within a package should always have the same name,
ending with C<_cb>.  For example, a hypothetical C<Amanda::Estimate> module might
provide its estimates through a callback with four parameters.  This callback
should be referred to as C<estimate_cb> throughout the package, and its
parameters should be clearly defined in the package's documentation.  It should
take positional parameters only.  If error conditions must also be communicated
via the callback, then the first parameter should be an C<$error> parameter,
which is undefined when no error has occurred.  The Changer API's C<res_cb> is
typical of such a callback signature.

A caller can only know that an operation is complete by the invocation of the
callback, so it is important that a callback be invoked I<exactly once> in all
circumstances.  Even in an error condition, the caller needs to know that the
operation has failed.  Also beware of bugs that might cause a callback to be
invoked twice.

Functions or methods taking callbacks as arguments should either take only a
callback (like C<call_later>), or take hash-key parameters, where the
callback's key is the signature name.  For example, the C<Amanda::Estimate>
package might define a function like C<perform_estimate>, invoked something
like this:

    my $estimate_cb = make_cb(estimate_cb => sub {
	my ($err, $size, $level) = @_;
	die $err if $err;
	# ...
    });
    Amanda::Estimate::perform_estimate(
	host => $host,
	disk => $disk,
	estimate_cb => $estimate_cb,
    );

When invoking a user-supplied callback within the library, there is no need to
wrap it in a C<call_later> invocation, as the user already supplied that
wrapper via C<make_cb>, or is not interested in using such a wrapper.

Callbacks are a form of continuation
(L<http://en.wikipedia.org/wiki/Continuations>), and as such should only be
called at the I<end> of a function.  Do not do anything after invoking a
callback, as you cannot know what processing has gone on in the callback.

    sub estimate_done {
	# ...
	$self->{'estimate_cb'}->(undef, $size, $level);
	$self->{'estimate_in_progress'} = 0; # BUG!!
    }

In this case, the C<estimate_cb> invocation may have called C<perform_estimate>
again, setting C<estimate_in_progress> back to 1.  A technique to avoid this
pitfall is to always C<return> a callback's result, even though that result is
not important.  This makes the bug much more apparent:

    sub estimate_done {
	# ...
	return $self->{'estimate_cb'}->(undef, $size, $level);
	$self->{'estimate_in_progress'} = 0; # BUG (this just looks silly)
    }

=head1 LOW-LEVEL INTERFACE

MainLoop events are generated by event sources.  A source may produce multiple
events over its lifetime.  The higher-level methods in the previous section
provide a more Perlish abstraction of event sources, but for efficiency it is
sometimes necessary to use event sources directly.

The method C<< $src->set_callback(\&cb) >> sets the function that will be called
for a given source, and "attaches" the source to the main loop so that it will
begin generating events.  The arguments to the callback depend on the event
source, but the first argument is always the source itself.  Unless specified,
no other arguments are provided.

Event sources persist until they are removed with C<< $src->remove() >>, even if
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

There is no means in place to specify extra arguments to be provided to a
source callback when it is set.  If the callback needs access to other data, it
should use a Perl closure in the form of lexically scoped variables and an
anonymous sub.  In fact, this is exactly what the higher-level functions
described above do.

=head2 Timeout

  my $src = Amanda::MainLoop::timeout_source(10000);

A timeout source will create events at the specified interval, specified in
milliseconds (thousandths of a second).  The events will continue until the
source is destroyed.

=head2 Idle

  my $src = Amanda::MainLoop::idle_source(2);

An idle source will create events continuously except when a higher-priority
source is emitting events.  Priorities are generally small positive integers,
with larger integers denoting lower priorities.  The events will continue until
the source is destroyed.

=head2 Child Watch

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

=head2 File Descriptor

  my $src = Amanda::MainLoop::fd_source($fd, $G_IO_IN);

This source will issue an event whenever one of the given conditions is true
for the given file (a file handle or integer file descriptor).  The conditions
are from Glib's GIOCondition, and are C<$G_IO_IN>, C<G_IO_OUT>, C<$G_IO_PRI>,
C<$G_IO_ERR>, C<$G_IO_HUP>, and C<$G_IO_NVAL>.  These constants are available
with the import tag C<:GIOCondition>.

Generally, when reading from a file descriptor, use
C<$G_IO_IN|$G_IO_HUP|$G_IO_ERR> to ensure that an EOF triggers an event as
well.  Writing to a file descriptor can simply use C<$G_IO_OUT|$G_IO_ERR>.

The callback attached to an FdSource should read from or write to the
underlying file descriptor before returning, or it will be called again in the
next iteration of the main loop, which can lead to unexpected results.  Do
I<not> use C<make_cb> here!

=head2 Combining Event Sources

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

=head2 Relationship to Glib

Glib's main event loop is described in the Glib manual:
L<http://library.gnome.org/devel/glib/stable/glib-The-Main-Event-Loop.html>.
Note that Amanda depends only on the functionality available in Glib-2.2.0, so
many functions described in that document are not available in Amanda.  This
module provides a much-simplified interface to the glib library, and is not
intended as a generic wrapper for it: Amanda's perl-accessible main loop only
runs a single C<GMainContext>, and always runs in the main thread; and (aside
from idle sources), event priorities are not accessible from Perl.

=cut
%}

%perlcode %{

use POSIX;

## basic functions

BEGIN {
    my $have_sub_name = eval "use Sub::Name; 1";
    if (!$have_sub_name) {
	eval <<'EOF'
	    sub subname {
		my ($name, $sub) = @_;
		$sub;
	    }
EOF
    }
}

# glib's g_is_main_loop_running() seems inaccurate, so we just
# track that information locally..
my $mainloop_running = 0;
sub run {
    $mainloop_running = 1;
    run_c();
    $mainloop_running = 0;
}

sub is_running {
    return $mainloop_running;
}

# quit is a direct call to C

## utility functions

my @waiting_to_call_later;
sub call_later {
    my ($sub, @args) = @_;

    # add the callback if nothing is waiting right now
    if (!@waiting_to_call_later) {
        timeout_source(0)->set_callback(sub {
            my ($src) = @_;
            $src->remove();

            while (@waiting_to_call_later) {
                my ($sub, @args) = @{shift @waiting_to_call_later};
                $sub->(@args) if $sub;
            }
        });
    }

    push @waiting_to_call_later, [ $sub, @args ];
}

sub make_cb {
    my ($name, $sub) = @_;

    if ($sub) {
	my ($pkg, $filename, $line) = caller;
	my $newname = sprintf('$%s::%s@l%s', $pkg, $name, $line);
	$sub = subname($newname => $sub);
    } else {
	$sub = $name; # no name => sub is actually in first parameter
    }

    sub {
        Amanda::MainLoop::call_later($sub, @_);
    };
}
push @EXPORT, 'make_cb';

sub call_after {
    my ($delay_ms, $sub, @args) = @_;

    my $src = timeout_source($delay_ms);
    $src->set_callback(sub {
	$src->remove();
	$sub->(@args);
    });

    return $src;
}

sub call_on_child_termination {
    my ($pid, $cb, @args) = @_;

    my $src = child_watch_source($pid);
    $src->set_callback(sub {
	my ($src, $pid, $exitstatus) = @_;
	$src->remove();
	return $cb->($exitstatus);
    });
}

sub async_read {
    my %params = @_;
    my $fd = $params{'fd'};
    my $size = $params{'size'} || 0;
    my $cb = $params{'async_read_cb'};
    my @args;
    @args = @{$params{'args'}} if exists $params{'args'};

    my $fd_cb = sub {
	my ($src) = @_;
	$src->remove();

	my $buf;
	my $res = POSIX::read($fd, $buf, $size || 32768);
	if (!defined $res) {
	    return $cb->($!, undef, @args);
	} else {
	    return $cb->(undef, $buf, @args);
	}
    };
    my $src = fd_source($fd, $G_IO_IN|$G_IO_HUP|$G_IO_ERR);
    $src->set_callback($fd_cb);
    return $src;
}

my %outstanding_writes;
sub async_write {
    my %params = @_;
    my $fd = $params{'fd'};
    my $data = $params{'data'};
    my $cb = $params{'async_write_cb'};
    my @args;
    @args = @{$params{'args'}} if exists $params{'args'};

    # more often than not, writes will not block, so just try it.
    if (!exists $outstanding_writes{$fd}) {
	my $res = POSIX::write($fd, $data, length($data));
	if (!defined $res) {
	    if ($! != POSIX::EAGAIN) {
		return $cb->($!, 0, @args);
	    }
	} elsif ($res eq length($data)) {
	    return $cb->(undef, $res, @args);
	} else {
	    # chop off whatever data was written
	    $data = substr($data, $res);
	}
    }

    if (!exists $outstanding_writes{$fd}) {
	my $fd_writes = $outstanding_writes{$fd} = [];
	my $src = fd_source($fd, $G_IO_OUT|$G_IO_HUP|$G_IO_ERR);

	# (note that this does not coalesce consecutive outstanding writes
	# into a single POSIX::write call)
	my $fd_cb = sub {
	    my $ow = $fd_writes->[0];
	    my ($buf, $nwritten, $len, $cb, $args) = @$ow;

	    my $res = POSIX::write($fd, $buf, $len-$nwritten);
	    if (!defined $res) {
		shift @$fd_writes;
		$cb->($!, $nwritten, @$args);
	    } else {
		$ow->[1] = $nwritten = $nwritten + $res;
		if ($nwritten == $len) {
		    shift @$fd_writes;
		    $cb->(undef, $nwritten, @$args);
		} else {
		    $ow->[0] = substr($buf, $res);
		}
	    }

	    # (the following is *intentionally* done after calling $cb, allowing
	    # $cb to add a new message to $fd_writes if desired, and thus avoid
	    # removing and re-adding the source)
	    if (@$fd_writes == 0) {
		$src->remove();
		delete $outstanding_writes{$fd};
	    }
	};

	$src->set_callback($fd_cb);
    }
    
    push @{$outstanding_writes{$fd}}, [ $data, 0, length($data), $cb, \@args ];
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
    int fd,
    GIOCondition events)
{
    GSource *fdsource = new_fdsource(fd, events);
    return amglue_source_new(fdsource,
	(GSourceFunc)amglue_source_callback_simple);
}
%}
