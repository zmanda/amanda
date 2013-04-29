package Amanda::JSON::RPC::Dispatcher::App;
BEGIN {
  $Amanda::JSON::RPC::Dispatcher::App::VERSION = '0.0505';
}

use Moose;
use Amanda::JSON::RPC::Dispatcher;

=head1 NAME

Amanda::JSON::RPC::Dispatcher::App - A base class for creating object oriented apps with JRD.

=head1 VERSION

version 0.0505

=head1 SYNOPSIS

Create your module:

 package MyApp;

 use Moose;
 extends 'Amanda::JSON::RPC::Dispatcher::App';

 sub sum {
    my ($self, @params) = @_;
    my $sum = 0;
    $sum += $_ for @params;
    return $sum;
 }

 sub guess {
    my ($self, $guess) = @_;
    if ($guess == 10) {
	    return 'Correct!';
    }
    elsif ($guess > 10) {
        confess [986, 'Too high.', $guess];
    }
    else {
        confess [987, 'Too low.', $guess];   
    }
 }

 __PACKAGE__->register_rpc_method_names( qw( sum guess ) );

 1;

Then your plack F<app.psgi>:

 MyApp->new->to_app;

=head1 DESCRIPTION

This package gives you a base class to make it easy to create object-oriented JSON-RPC applications. This is a huge benefit when writing a larger app or suite of applications rather than just exposing a procedure or two. If you build out classes of methods using Amanda::JSON::RPC::Dispatcher::App, and then use L<Plack::App::URLMap> to mount each module on a different URL, you can make a pretty powerful application server in very little time.

=head1 METHODS

The following methods are available from this class.

=head2 new ( )

A L<Moose> generated constructor.

When you subclass you can easily add your own attributes using L<Moose>'s C<has> function, and they will be accessible to your RPCs like this:

 package MyApp;

 use Moose;
 extends 'Amanda::JSON::RPC::Dispatcher::App';

 has db => (
    is          => 'ro',
    required    => 1,
 );

 sub make_it_go {
     my ($self, @params) = @_;
     my $sth = $self->db->prepare("select * from foo");
     ...
 }

 __PACKAGE__->register_rpc_method_names( qw(make_it_go) );

 1;

In F<app.psgi>:

 my $db = DBI->connect(...);
 MyApp->new(db=>$db)->to_app;

=cut

#--------------------------------------------------------

=head2 register_rpc_method_names ( names )

Class method. Registers a list of method names using L<Amanda::JSON::RPC::Dispatcher>'s C<register> method.

 __PACKAGE__->register_rpc_method_names( qw( add subtract multiply divide ));

=head3 names

The list of method names to register. If you want to use any registration options with a particular method you can do that by passing the method in as a hash reference like so:

 __PACKAGE__->register_rpc_method_names(
     'add',
     { name => 'ip_address', options => { with_plack_request => 1 } },
     'concat',
 );

=cut

sub _rpc_method_names {
    return ();
}

sub register_rpc_method_names {
    my ($class, @methods) = @_;
    $class->meta->add_around_method_modifier('_rpc_method_names', sub {
        my ($orig, $self) = @_;
        return ($orig->(), @methods);
    });
}

#--------------------------------------------------------

=head2 to_app ( )

Generates a PSGI/L<Plack> compatible app.

=cut

sub to_app {
    my $self = shift;
    my $rpc = Amanda::JSON::RPC::Dispatcher->new;
    my $ref;
    if ($ref = $self->can('_rpc_method_names')) {
        foreach my $method ($ref->()) {
            if (ref $method eq 'HASH') {
                my $name = $method->{name};
                $rpc->register($name, sub { $self->$name(@_) }, $method->{options});
            }
            else {
                $rpc->register($method, sub { $self->$method(@_) });
            }
        }
    }
    $rpc->to_app;
}


=head1 LEGAL

Amanda::JSON::RPC::Dispatcher is Copyright 2009-2010 Plain Black Corporation (L<http://www.plainblack.com/>) and is licensed under the same terms as Perl itself.

=cut

1;
