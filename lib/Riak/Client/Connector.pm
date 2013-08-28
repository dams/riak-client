## no critic (RequireUseStrict, RequireUseWarnings)
package Riak::Client::Connector;
## use critic

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
    my $sended = 0;
    do {
        $sended = $self->socket->syswrite( $bytes, $length, $offset );

        # error in $!
        return unless defined $sended;

        # test if $sended == 0 and $! EAGAIN, EWOULDBLOCK, ETC...
        return unless $sended;

        $offset += $sended;
    } while ( $offset < $length );

    $offset;
}

sub _read_all {
    my ( $self, $bufsiz ) = @_;

    my $buffer;
    my $offset = 0;
    my $readed = 0;
    do {
        $readed = $self->socket->sysread( $buffer, $bufsiz, $offset );

        # error in $!
        return unless defined $readed;

        # test if $sended == 0 and $! EAGAIN, EWOULDBLOCK, ETC...
        return unless $readed;

        $offset += $readed;
    } while ( $offset < $bufsiz );

    $buffer;
}

1;

__END__

=head1 DESCRIPTION
  
  Internal class
