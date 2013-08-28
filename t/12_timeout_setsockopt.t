BEGIN {
    use Config;
    if ( $Config{osname} eq 'netbsd' || $Config{osname} eq 'solaris') {
        require Test::More;
        Test::More::plan( skip_all =>
              'should not test Riak::Client::Timeout::SetSockOpt under Solaris OR Netbsd 6.0 (or superior) and longsize 4'
        );
    }
}

use Test::More tests => 6;
use FindBin qw($Bin);
use lib "$Bin/tlib";
use TestTimeout qw(test_timeout test_normal_wait);
use Test::MockModule;
use Test::MockObject;
use Test::Exception;

subtest "test die under solaris" => sub {

    use Riak::Client::Timeout::SetSockOpt;
    my $module = Test::MockModule->new('Riak::Client::Timeout::SetSockOpt');

    $module->mock( is_netbsd => 0 );
    $module->mock( is_solaris => 1 );

    throws_ok {
        Riak::Client::Timeout::SetSockOpt->new( socket => undef );
    }
    qr/Solaris is not supported/;

};

subtest "test die under netbsd 6" => sub {

    use Riak::Client::Timeout::SetSockOpt;
    my $module = Test::MockModule->new('Riak::Client::Timeout::SetSockOpt');

    $module->mock( is_netbsd => 1 );

    throws_ok {
        Riak::Client::Timeout::SetSockOpt->new( socket => undef );
    }
    qr/NetBSD no supported yet/;

};

subtest "test die if setsockopt fails for SO_RCVTIMEO" => sub {
    use Riak::Client::Timeout::SetSockOpt;

    my $mock = Test::MockObject->new();

    $! = 13;
    $mock->set_false('setsockopt');

    throws_ok {
        Riak::Client::Timeout::SetSockOpt->new( socket => $mock );
    }
    qr/setsockopt\(SO_RCVTIMEO\): $!/;
};

subtest "test die if setsockopt fail for SO_SNDTIMEO" => sub {
    use Riak::Client::Timeout::SetSockOpt;

    my $mock = Test::MockObject->new();

    $! = 13;
    $mock->set_series( 'setsockopt', 1, 0 );

    throws_ok {
        Riak::Client::Timeout::SetSockOpt->new( socket => $mock );
    }
    qr/setsockopt\(SO_SNDTIMEO\): $!/;
};

test_timeout('Riak::Client::Timeout::SetSockOpt');
test_normal_wait('Riak::Client::Timeout::SetSockOpt');
