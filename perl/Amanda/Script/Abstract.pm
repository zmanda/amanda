# This copyright apply to all codes written by authors that made contribution
# made under the BSD license.  Read the AUTHORS file to know which authors made
# contribution made under the BSD license.
#
# The 3-Clause BSD License

# Copyright 2017 Purdue University
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

use strict;
use warnings;

=head1 NAME

Amanda::Script::Abstract - A base class for Amanda scripts

=head1 SYNOPSIS

    package amMyScript;
    use base qw(Amanda::Script::Abstract)

    package main;
    amMyScript::->run();

=head1 DESCRIPTION

C<Amanda::Script::Abstract> handles much of the common housekeeping
needed to implement Amanda's
L<Script API|http://wiki.zmanda.com/index.php/Script_API>, so that
practical applications can be implemented in few lines of code by overriding
only a few necessary methods.

C<Amanda::Script::Abstract> is itself a subclass of
C<Amanda::Script>, but the latter cannot be instantiated until after
the command line has been parsed (at least the I<execute-where> argument and
C<config> options need to be
passed to its constructor). Therefore, C<Amanda::Script::Abstract>
supplies I<class> methods for the preliminary work of declaring the supported
options, parsing the command line, and finally calling C<new> to get an instance
of the class. Then the actual operations of the script are carried out by
instance methods, as you would expect.

In perl, class methods are inheritable and overridable just as instance methods
are, and the syntax C<$class-E<gt>SUPER::method(...)> works, analogously to its
familiar use in an instance method. So, the pre-instantiation behavior of a
script (declaring command options, etc.) can be tailored by simply
overriding the necessary class methods, just as its later operations can be
supplied by overriding instance methods.

=cut

package Amanda::Script::Abstract;
use base qw(Amanda::Script);

use Amanda::Feature;
use Data::Dumper;
use File::Path qw{make_path};
use File::Spec;
use Getopt::Long;
use IO::Handle;
use Scalar::Util qw{blessed};
use Text::ParseWords;

=head1 FUNDAMENTAL CLASS METHODS

=head2 C<run>

    scriptclass::->run()

Run the script: grab the I<execute-where> from C<ARGV[0]>, be sure it is a known
one, set up for option parsing, parse the options, construct an instance
passing all the supplied options (which will include the config name, needed
by the C<Amanda::Script> constructor), and finally C<do()> the I<execute-where>,
where C<do> is inherited from C<Amanda::Script_App> by way of
C<Amanda::Script>.

=cut

sub run {
    my ( $class ) = @_;
    my $execute_where = shift @ARGV || '(empty)';

    my %opthash = ();
    my @optspecs = ();

    $class->declare_options(\%opthash, \@optspecs);

    my $ret = GetOptions(\%opthash, @optspecs);

    # !!! pleasant surprise: option value arrived with escaping already
    # unwrapped. Examining sendbackup debug log shows the value apparently
    # passed to sendbackup in an escaped form, but unescaped in the section
    # of the log file that shows "sendbackup: running ..." with the arguments
    # logged one by one. Looks like sendbackup does the unwrapping and uses
    # exec directly; todo: check the code to confirm.

    # ... yes, that's just what sendbackup does.

    my $script = $class->new($execute_where, \%opthash);

    $script->do($execute_where);
}

=head2 C<new>

    $class->new(\%opthash)

A typical application need not call this; it is called by C<run> after the
command line options have been declared and the command line has been parsed.
The hash reference contains option values as stored by C<GetOptions>. A certain
convention is assumed: if there is an option/property named I<o>, its value
will be stored in I<opthash> with key exactly I<o>, in exactly the way
C<GetOptions> normally would, I<unless> the option-declaring method had planted
a code reference there in order to more strictly validate the option. In that
case, of course I<$opthash{o}> is the code reference, and the final value will
be stored with key C<opt_>I<o> instead. Because of that convention, entries in
I<opthash> with an C<opt_> prefix will otherwise be ignored; avoid declaring
options/properties whose actual names start with C<opt_>.

=cut

sub new {
    my ( $class, $execute_where, $refopthash ) = @_;
    my %options = ();
    for ( my ($k, $v) ; ($k, $v) = each %{$refopthash} ; ) {
	next if $k =~ /^opt_/;
	if ( "CODE" ne ref $v ) {
	    $options{$k} = $v;
	}
	else {
	    $options{$k} = $refopthash->{"opt_".$k}
	}
    }
    my $self = $class->SUPER::new($execute_where, $options{'config'});
    $self->{'options'} = \%options;
    return $self;
}

=head1 CLASS METHOD FOR DECLARING ALLOWED OPTIONS / PROPERTIES

=head2 C<declare_options>

    declare_options(\%opthash, \@optspecs)

This is a class method that is passed two references: a hash reference
I<\%opthash> and a list reference I<\@optspecs>. It should push onto
I<\@optspecs> the declarations of whatever options are valid for the
script, using the syntax described in L<Getopt::Long>.
It does not need to touch I<\%opthash>, but may store a code reference at the
name of any option, to apply stricter validation when the option is parsed.
In that case, the code reference should ultimately store the validated value
into I<\%opthash> at a key derived by prefixing C<opt_> to the option name.

=cut

sub declare_options {
    my ( $class, $refopthash, $refoptspecs ) = @_;
    push @$refoptspecs,
        ( 'config=s', 'host=s', 'disk=s', 'device=s', 'level=i@',
	  'execute-where=s' );
}

1;
