# Copyright (c) 2024-2025, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use File::Path qw(mkpath);
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Copy;

my $dlsuffix;

if ($^O eq 'darwin') {
    $dlsuffix = 'dylib';
}
elsif ($^O eq 'MSWin32') {
    $dlsuffix = 'dll';
}
else {
    $dlsuffix = 'so';
}
my $node = PostgreSQL::Test::Cluster->new('node');
my $user = "user01";
my $libdir = `pg_config --pkglibdir`;
my $sharedir = `pg_config --sharedir`;
chomp $libdir;
chomp $sharedir;
mkpath("$libdir/plugins");

$node->init;

# Create a temporary directory for the extension control file
my $ext_dir = PostgreSQL::Test::Utils::tempdir();
my $dyn_library_path = "$ext_dir/lib";
mkpath("$ext_dir/extension");
mkpath($dyn_library_path);

# Use the correct separator and escape \ when running on Windows.
my $sep = $windows_os ? ";" : ":";
$node->append_conf(
	'postgresql.conf', qq{
dynamic_library_path = '@{[ $windows_os ? ($dyn_library_path =~ s/\\/\\\\/gr) : $dyn_library_path ]}$sep\$libdir'
extension_control_path = '@{[ $windows_os ? ($ext_dir =~ s/\\/\\\\/gr) : $ext_dir ]}$sep\$system'
});

$node->start;

# Create a non-superuser user.
$node->safe_psql('postgres', "CREATE USER $user");

# --------------------------
# Safe path. It should expand the dynamic library file from the $libdir as it
# is specified on dummy_index_am.control and also on LOAD command.
my $ret = $node->psql('postgres', "CREATE EXTENSION dummy_index_am");
is($ret, 0, "create extension");
$ret = $node->psql('postgres', "LOAD '\$libdir/dummy_index_am'");
is($ret, 0, "LOAD dynamic module from \$libdir");

$ret = $node->psql('postgres', "LOAD 'dummy_index_am'");
is($ret, 0, "LOAD dynamic module");

# --------------------------
# Create an invalid dynamic library file so that we can ensure that we can
# still load from $libdir if we want.
open my $fh, '>', "$dyn_library_path/dummy_index_am.$dlsuffix" or die "Cannot create file: $!";
close $fh;

$ret = $node->psql('postgres', "LOAD '\$libdir/dummy_index_am'");
is($ret, 0, "LOAD the correctly dynamic module from the \$libdir");

# --------------------------
# Copy the auto_explain from $libdir to $libdir/plugins so non-superusers can
# LOAD
copy("$libdir/auto_explain.$dlsuffix", "$libdir/plugins/auto_explain.$dlsuffix") or die "Copy failed: $!";

$ret = $node->psql('postgres', "LOAD '\$libdir/plugins/auto_explain'",
	connstr => $node->connstr('postgres') . qq' user=$user');
is($ret, 0, "LOAD dynamic module from the \$libdir/plugins");

# --------------------------
# Move the dummy_index_am from $system and $libdir directory to a custom
# directory to ensure that we can still load the dynamic library files that has
# the $libdir/ prefix on module_pathname.
$ret = $node->psql('postgres', "DROP EXTENSION dummy_index_am");
is($ret, 0, "drop extension");

copy("$sharedir/extension/dummy_index_am.control", "$ext_dir/extension/dummy_index_am.control") or die "Copy failed: $!";
copy("$sharedir/extension/dummy_index_am--1.0.sql", "$ext_dir/extension/dummy_index_am--1.0.sql") or die "Copy failed: $!";
copy("$libdir/dummy_index_am.$dlsuffix", "$dyn_library_path/dummy_index_am.$dlsuffix") or die "Copy failed: $!";
unlink("$libdir/dummy_index_am.$dlsuffix");

$ret = $node->psql('postgres', "CREATE EXTENSION dummy_index_am");
is($ret, 0, "create extension from custom extension control path");

# Copy again to $libdir to make it accessible to other tests
copy("$dyn_library_path/dummy_index_am.$dlsuffix", "$libdir/dummy_index_am.$dlsuffix",) or die "Copy failed: $!";

done_testing();
