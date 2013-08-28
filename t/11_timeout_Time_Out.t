BEGIN {
    use Config;
    if ( $Config{osname} eq 'MSWin32' ) {
        require Test::More;
        Test::More::plan( skip_all =>
              'should not test Riak::Client::Timeout::TimeOut under Win32' );
    }
}

use Test::More tests => 3;
use FindBin qw($Bin);
use lib "$Bin/tlib";
use TestTimeout qw(test_timeout test_normal_wait);
use Test::MockModule;
use Test::Exception;

subtest "test die under win32" => sub {

    use Riak::Client::Timeout::TimeOut;
    my $module = Test::MockModule->new('Riak::Client::Timeout::TimeOut');

    $module->mock( is_windows => 1 );

    throws_ok {
        Riak::Client::Timeout::TimeOut->new( socket => undef );
    }
    qr/Time::Out alarm\(2\) doesn't interrupt blocking I\/O on MSWin32/;

};

test_timeout('Riak::Client::Timeout::TimeOut');
test_normal_wait('Riak::Client::Timeout::TimeOut');
