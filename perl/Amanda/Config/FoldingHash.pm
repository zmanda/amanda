package Amanda::Config::FoldingHash;
use Tie::Hash;
use base 'Tie::StdHash';
require Amanda::Config;
require Amanda::Debug;

use strict;
use warnings;

sub new {
    my ($class) = @_;

    my %self;
    tie(%self, $class);
    return \%self;
}

sub _amandaify {
    return Amanda::Config::amandaify_property_name(@_);
}

sub TIEHASH {
    my ($class) = @_;
    return bless({}, $class);
}

sub FETCH {
    my ($self, $key) = @_;
    my $am = _amandaify($key);
    return $self->{_amandaify($key)};
}

sub STORE {
    my ($self, $key, $value) = @_;
    return $self->{_amandaify($key)} = $value;
}

sub EXISTS {
    my ($self, $key) = @_;
    return exists($self->{_amandaify($key)});
}

sub DELETE {
    my ($self, $key) = @_;
    return delete($self->{_amandaify($key)});
}

1;
