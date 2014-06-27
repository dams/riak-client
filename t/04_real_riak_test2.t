use strict;
use warnings;
BEGIN {
    unless ( $ENV{RIAK_PBC_HOST} ) {
        require Test::More;
        Test::More::plan(
            skip_all => 'variable RIAK_PBC_HOST is not defined' );
    }
}

use Test::More;
use Test::Exception;
use Riak::Client;
use JSON;

use AnyEvent;

my ( $host, $port ) = split ':', $ENV{RIAK_PBC_HOST};

my $client1 = Riak::Client->new(
    anyevent_mode => 1,
    host => $host,
    port => $port,
);

my $client2 = Riak::Client->new(
    anyevent_mode => 1,
    host => $host,
    port => $port,
);

my $cv = AE::cv;

foreach my $i (0..50) {
    $cv->begin;
    $client1->put(bucket => 'foo1' => ['bar'], sub {
                      # say STDERR "C1 PUT";
                      $cv->end; });
    $cv->begin;
    $client1->get(bucket => 'foo1', sub {
                      # say STDERR "C1 GET :" . ($_[0] // [ '<undef>' ])->[0];
                      $cv->end});
    $cv->begin;
    $client2->put(bucket => 'foo2' => ['bar'], sub {
                      # say STDERR "C2 PUT";
                      $cv->end; });
    $cv->begin;
    $client2->get(bucket => 'foo2', sub {
                      # say STDERR "C2 GET :" . ($_[0] // [ '<undef>' ])->[0];
                      $cv->end });
}

$cv->recv();

ok(1);

done_testing;
