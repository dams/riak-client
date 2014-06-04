package Riak::Client::Connector;

use Moo;
use Errno qw(EINTR);
use Types::Standard -types;
require bytes;
use IO::Socket;
use IO::Socket::Timeout;
use Carp;
use Type::Params qw(compile);
use Types::Standard -types;

# ABSTRACT: Riak Connector, abstraction to deal with binary messages

has host    => ( is => 'ro', isa => Str,  required => 1 );
has port    => ( is => 'ro', isa => Int,  required => 1 );
has connection_timeout => ( is => 'ro',                 isa => Num,  default  => sub {5} );
has read_timeout       => ( is => 'ro', predicate => 1, isa => Num,  default  => sub {5} );
has write_timeout      => ( is => 'ro', predicate => 1, isa => Num,  default  => sub {5} );

has _socket => ( is => 'ro', lazy => 1, builder => 1 );

sub _build__socket {
    my ($self) = @_;

    my $host = $self->host;
    my $port = $self->port;

    my $socket = IO::Socket::INET->new(
        PeerHost => $host,
        PeerPort => $port,
        Timeout  => $self->connection_timeout,
    );

    croak "Error ($!), can't connect to $host:$port"
      unless defined $socket;

    # enable read and write timeouts on the socket
    IO::Socket::Timeout->enable_timeouts_on($socket);
    # setup the timeouts
    $socket->read_timeout($self->read_timeout);
    # $socket->write_timeout($self->write_timeout);

    return $socket;
}

sub perform_request {
    my ( $self, $message ) = @_;
    my $bytes = pack( 'N a*', bytes::length($message), $message );

    $self->_send_all($bytes);    # send request
}

sub read_response {
    my ($self)   = @_;
    my $length = $self->_read_length();    # read first four bytes
    return unless ($length);
    $self->_read_all($length);             # read the message
}

sub _read_length {
    my ($self)   = @_;

    my $first_four_bytes = $self->_read_all(4);

    return unpack( 'N', $first_four_bytes ) if defined $first_four_bytes;

    undef;
}

sub _send_all {
    my ( $self, $bytes ) = @_;

    my $length = bytes::length($bytes);
    my $offset = 0;
    my $sent = 0;
    my $socket = $self->_socket;

    while ($length > 0) {
        $sent = $socket->syswrite( $bytes, $length, $offset );
        if (! defined $sent) {
            $! == EINTR
              and next;
            return;
        }

        $sent > 0
          or return;

        $offset += $sent;
        $length -= $sent;
    }

    return $offset;
}

sub _read_all {
    my ( $self, $length ) = @_;

    my $buffer;
    my $offset = 0;
    my $read = 0;
    my $socket = $self->_socket;

    while ($length > 0) {
        $read = $socket->sysread( $buffer, $length, $offset );
        if (! defined $read) {
            $! == EINTR
              and next;
            return;
        }

        $read > 0
          or return;

        $offset += $read;
        $length -= $read;
    }

    return $buffer;
}

1;
