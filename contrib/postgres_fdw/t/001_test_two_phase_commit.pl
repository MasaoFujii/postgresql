# Tests for two phase commit
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 9;

# Setup a coordinator node
my $node_master = PostgresNode->new("master");
$node_master->init(allows_streaming => 1);
$node_master->append_conf('postgresql.conf', qq(
postgres_fdw.two_phase_commit = true
log_statement = 'all'
synchronous_standby_names ='*'
));
$node_master->start;

# Setup a standby node for coordinator
my $node_standby = PostgresNode->new("standby");
my $backup_name = 'master_backup';
$node_master->backup($backup_name);
$node_standby->init_from_backup($node_master, $backup_name,
							   has_streaming => 1);
$node_standby->start;
$node_master->reload;

# Set up foreign nodes
my @foreign_nodes = setup_foreign_nodes(3);

# Prepare remote accesses
$node_master->safe_psql('postgres', qq(
CREATE EXTENSION postgres_fdw
));
foreach my $f_node (@foreign_nodes)
{
	my $tbl_name = $f_node->name;
	my $srv_name = $f_node->name;
	my $port = $f_node->port;

	# Create a foreign server object
	$node_master->safe_psql('postgres', qq(
	CREATE SERVER $srv_name FOREIGN DATA WRAPPER postgres_fdw
	OPTIONS (dbname 'postgres', port '$port');
	));
	
	# Create a user mapping
	$node_master->safe_psql('postgres', qq(
	CREATE USER MAPPING FOR CURRENT_USER SERVER $srv_name;
	));

	# Create a foreign table
	$f_node->safe_psql('postgres', qq(
	CREATE SCHEMA fs;
	CREATE TABLE fs.$tbl_name (c int);
	));
	$node_master->safe_psql('postgres', qq(
	IMPORT FOREIGN SCHEMA fs FROM SERVER $srv_name INTO public;
	));
}

# Create a local table
$node_master->safe_psql('postgres', qq(
CREATE TABLE l_table (c int);
));

# Check current values for tests
my @user_mapping_oids;
foreach my $f_node (@foreign_nodes)
{
	my $srv_name = $f_node->name;
	my $umid = $node_master->safe_psql('postgres', qq(
	SELECT umid FROM pg_user_mappings WHERE srvname = '$srv_name';
	));
	push @user_mapping_oids, $umid;
}

# Test patterns
my $tbl1 = $foreign_nodes[0]->name;
my $tbl2 = $foreign_nodes[1]->name;
my $tbl3 = $foreign_nodes[2]->name;

# Insert all foreign tables
test_if_two_phase_commit_used(qq(
BEGIN;
INSERT INTO $tbl1 VALUES (1);
INSERT INTO $tbl2 VALUES (1);
INSERT INTO $tbl3 VALUES (1);
COMMIT;
), ("true", "true", "true"));

# Insert all foreign tables and execute select queries
test_if_two_phase_commit_used(qq(
BEGIN;
INSERT INTO $tbl1 VALUES (1);
SELECT * FROM $tbl1;
INSERT INTO $tbl2 VALUES (1);
SELECT * FROM $tbl2;
INSERT INTO $tbl3 VALUES (1);
SELECT * FROM $tbl3;
COMMIT;
), ("true", "true", "true"));

# Insert two foreign tables
test_if_two_phase_commit_used(qq(
BEGIN;
INSERT INTO $tbl1 VALUES (1);
INSERT INTO $tbl2 VALUES (1);
COMMIT;
), ("true", "true", "false"));

## Insert local table and a foregin table
#test_two_phase_commit_used(qq(
#BEGIN;
#INSERT INTO $tbl1 VALUES (1);
#INSERT INTO l_table VALUES (1);
#COMMIT;
#), ("true", "false", "false"));
#
## Only Select queries are executed on foreign tables
#test_two_phase_commit_used(qq(
#BEGIN;
#SELECT * FROM $tbl1;
#SELECT * FROM $tbl2;
#SELECT * FROM $tbl3;
#COMMIT;
#), ("false", "false", "false"));
#
## Only a insert query is executed on foreign tables
#test_two_phase_commit_used(qq(
#BEGIN;
#INSERT INTO $tbl1 VALUES (1);
#COMMIT;
#), ("false", "false", "false"));


# Initialize foreign nodes
sub setup_foreign_nodes
{
	my ($num) = @_;
	
	my @nodes;
	foreach my $i (1..$num)
	{
		my $node = new_foreign_node("fs$i");
		push @nodes, $node;
	}
	return @nodes;
}

# Initialize a foreign node
sub new_foreign_node
{
	my ($name) = @_;

	my $node = PostgresNode->new($name);
	$node->init;
	$node->append_conf('postgresql.conf', qq(
	log_statement = 'all'
	max_prepared_transactions = 10
	));
	$node->start;

	return $node;
}

# Verify that two phase commit is used or not
sub test_if_two_phase_commit_used
{
	my ($sql, @used) = @_;

	my $txid_current = $node_master->safe_psql('postgres', qq(
	SELECT txid_current();
	));
	$txid_current++;  # prepared transaction id must have next txid
	
	# Execute the sql query
	$node_master->safe_psql('postgres', $sql);

	# Prepare to verify
	my $cluster_name = $node_master->name;  # cluster name is configured to the argument of PostgresNode->new()
	
	# Verify for each node using its log
	while (my ($i, $f_node) = each(@foreign_nodes))
	{
		my $f_umid = ${user_mapping_oids[$i]};
		my $f_node_log = slurp_file($f_node->logfile());
		my $prepare_txid = "'pgfdw_${txid_current}_${f_umid}_${cluster_name}'";

		if ($used[$i] eq 'true')
		{
			like(
				$f_node_log,
				qr/COMMIT PREPARED $prepare_txid/,
				$prepare_txid
			);
		}
		else
		{
			unlike(
				$f_node_log,
				qr/COMMIT PREPARED $prepare_txid/,
				$prepare_txid
			);
		}
	}
} 
