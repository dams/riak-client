## no critic (RequireUseStrict, RequireUseWarnings)
package Riak::Client::Timeout::SetSockOpt;
## use critic

use POSIX qw(ETIMEDOUT ECONNRESET);
use Socket;
use IO::Select;
use Time::HiRes;
use Riak::Client::Util qw(is_netbsd is_solaris);
use Carp;
use Moo;
use Types::Standard -types;

with 'Riak::Client::Timeout';

# ABSTRACT: proxy to read/write using IO::Select as a timeout provider only for READ operations.

has socket      => ( is => 'ro', required => 1 );
has in_timeout  => ( is => 'ro', isa      => Num, default => sub {0.5} );
has out_timeout => ( is => 'ro', isa      => Num, default => sub {0.5} );
has is_valid    => ( is => 'rw', isa      => Bool, default => sub {1} );

sub BUILD {
    # carp "This Timeout Provider is EXPERIMENTAL!";

    croak "NetBSD no supported yet"
      if is_netbsd();
    ## TODO: see https://metacpan.org/source/ZWON/RedisDB-2.12/lib/RedisDB.pm#L235
      
    croak "Solaris is not supported"
      if is_solaris();
    
    $_[0]->_set_so_rcvtimeo();
    $_[0]->_set_so_sndtimeo();
}

sub _set_so_rcvtimeo {
    my ($self) = @_;
    my $seconds  = int( $self->in_timeout );
    my $useconds = int( 1_000_000 * ( $self->in_timeout - $seconds ) );
    my $timeout  = pack( 'l!l!', $seconds, $useconds );

    $self->socket->setsockopt( SOL_SOCKET, SO_RCVTIMEO, $timeout )
      or croak "setsockopt(SO_RCVTIMEO): $!";
}

sub _set_so_sndtimeo {
    my ($self) = @_;
    my $seconds  = int( $self->out_timeout );
    my $useconds = int( 1_000_000 * ( $self->out_timeout - $seconds ) );
    my $timeout  = pack( 'l!l!', $seconds, $useconds );

    $self->socket->setsockopt( SOL_SOCKET, SO_SNDTIMEO, $timeout )
      or croak "setsockopt(SO_SNDTIMEO): $!";
}

sub clean {
    $_[0]->socket->close();
    $_[0]->is_valid(0);
    $! = ETIMEDOUT;         ## no critic (RequireLocalizedPunctuationVars)
}

sub sysread {
    my $self = shift;
    $self->is_valid or $! = ECONNRESET, return;    ## no critic (RequireLocalizedPunctuationVars)

    my $result = $self->socket->sysread(@_);

    $self->clean() unless ($result);

    $result;
}

sub syswrite {
    my $self = shift;
    $self->is_valid or $! = ECONNRESET, return;    ## no critic (RequireLocalizedPunctuationVars)

    my $result = $self->socket->syswrite(@_);

    $self->clean() unless ($result);

    $result;
}

1;

__END__

=head1 DESCRIPTION
  
  Internal class
