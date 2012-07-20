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

%module "Amanda::MainLoop"
%include "amglue/amglue.swg"
%include "exception.i"

%include "Amanda/MainLoop.pod"

%{
#include "amanda.h"
#include "event.h"
%}

%perlcode %{

use POSIX;
use Carp;

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
push @EXPORT_OK, "run";

sub is_running {
    return $mainloop_running;
}
push @EXPORT_OK, "is_running";

# quit is a direct call to C
push @EXPORT_OK, "quit";

## utility functions

my @waiting_to_call_later;
sub call_later {
    my ($sub, @args) = @_;

    confess "undefined sub" unless ($sub);

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
push @EXPORT_OK, "call_later";

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

    confess "undefined sub" unless ($sub);

    my $src = timeout_source($delay_ms);
    $src->set_callback(sub {
	$src->remove();
	$sub->(@args);
    });

    return $src;
}
push @EXPORT_OK, "call_after";

sub call_on_child_termination {
    my ($pid, $cb, @args) = @_;

    confess "undefined sub" unless ($cb);

    my $src = child_watch_source($pid);
    $src->set_callback(sub {
	my ($src, $pid, $exitstatus) = @_;
	$src->remove();
	return $cb->($exitstatus);
    });
}
push @EXPORT_OK, "call_on_child_termination";

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
push @EXPORT_OK, "async_read";

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
push @EXPORT_OK, "async_write";

sub synchronized {
    my ($lock, $orig_cb, $sub) = @_;
    my $continuation_cb;

    $continuation_cb = sub {
	my @args = @_;

	# shift this invocation off the queue
	my ($last_sub, $last_orig_cb) = @{ shift @$lock };

	# start the next invocation, if the queue isn't empty
	if (@$lock) {
	    Amanda::MainLoop::call_later($lock->[0][0], $continuation_cb);
	}

	# call through to the original callback for the last invocation
	return $last_orig_cb->(@args);
    };

    # push this sub onto the lock queue
    if ((push @$lock, [ $sub, $orig_cb ]) == 1) {
	# if this is the first addition to the queue, start it
	$sub->($continuation_cb);
    }
}
push @EXPORT_OK, "synchronized";

{   # privat variables to track the "current" step definition
    my $current_steps;
    my $immediate;
    my $first_step;

    sub define_steps (@) {
	my (%params) = @_;
	my $cb_ref = $params{'cb_ref'};
	my $finalize = $params{'finalize'};
	my %steps;

	croak "cb_ref is undefined" unless defined $cb_ref;
	croak "cb_ref is not a reference" unless ref($cb_ref) eq 'REF';
	croak "cb_ref is not a code double-reference" unless ref($$cb_ref) eq 'CODE';

	# arrange to clear out $steps when $exit_cb is called; this eliminates
	# reference loops (values in %steps are closures which point to %steps).
	# This also clears $current_steps, which is likely holding a reference to
	# the steps hash.
	my $orig_cb = $$cb_ref;
	$$cb_ref = sub {
	    %steps = ();
	    $current_steps = undef;
	    $finalize->() if defined($finalize);
	    goto $orig_cb;
	};

	# set up state
	$current_steps = \%steps;
	$immediate = $params{'immediate'};
	$first_step = 1;

	return $current_steps;
    }
    push @EXPORT, "define_steps";

    sub step (@) {
	my (%params) = @_;
	my $step_immediate = $immediate || $params{'immediate'};
	delete $params{'immediate'} if $step_immediate;

	my ($name) = keys %params;
	my $cb = $params{$name};

	croak "expected a sub at key $name" unless ref($cb) eq 'CODE';

	# make the sub delayed
	unless ($step_immediate) {
	    my $orig_cb = $cb;
	    $cb = sub { Amanda::MainLoop::call_later($orig_cb, @_); }
	}

	# patch up the callback
	my ($pkg, $filename, $line) = caller;
	my $newname = sprintf('$%s::%s@l%s', $pkg, $name, $line);
	$cb = subname($newname => $cb);

	# store the step for later
	$current_steps->{$name} = $cb;

	# and invoke it, if it's the first step given
	if ($first_step) {
	    if ($step_immediate) {
		call_later($cb);
	    } else {
		$cb->();
	    }
	}
	$first_step = 0;
    }
    push @EXPORT, "step";
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
		SvREFCNT_dec(self->callback_sv);
		self->callback_sv = NULL;
	    }
	    self->callback_sv = newSVsv(callback_sub);
	    SvREFCNT_inc(self->callback_sv);
	    g_source_set_callback(self->src, self->callback,
		(gpointer)self, NULL);
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

    /* keep the source around long enough for the call to finish */
    amglue_source_ref(src);
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

    /* we no longer need the src */
    amglue_source_unref(src);
    src = NULL;

    /* this may have been freed, so don't use them after this point */
    src_sv = NULL;

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

    /* keep the source around long enough for the call to finish */
    amglue_source_ref(src);
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

    /* we no longer need the src */
    amglue_source_unref(src);
    src = NULL;

    /* this may have been freed, so don't use them after this point */
    src_sv = NULL;

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
