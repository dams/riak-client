package Riak::Client::Connector;

use Moo;
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
    do {
        $sent = $self->socket->syswrite( $bytes, $length, $offset );

        # error in $!
        return unless defined $sent;

        # TODO test if $sent == 0 and $! EAGAIN, EWOULDBLOCK, ETC...
        return unless $sent;

        $offset += $sent;
    } while ( $offset < $length );

    $offset;
}

sub _read_all {
    my ( $self, $bufsiz ) = @_;

    my $buffer;
    my $offset = 0;
    my $read = 0;
    do {
        $read = $self->socket->sysread( $buffer, $bufsiz, $offset );

        # error in $!
        return unless defined $read;

        # test if $read == 0 and $! EAGAIN, EWOULDBLOCK, ETC...
        return unless $read;

        $offset += $read;
    } while ( $offset < $bufsiz );

    $buffer;
}

1;
