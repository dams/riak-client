package Riak::Client::Connector;

use Moo;
use Errno qw(EINTR);
use Types::Standard -types;
require bytes;

# ABSTRACT: Riak Connector, abstraction to deal with binary messages

has socket => ( is => 'ro', required => 1 );

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

    while ($length > 0) {
        $sent = $self->socket->syswrite( $bytes, $length, $offset );
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

    while ($length > 0) {
        $read = $self->socket->sysread( $buffer, $length, $offset );
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
