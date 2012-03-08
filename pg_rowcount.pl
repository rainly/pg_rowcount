#!/usr/bin/perl
use DBI;
use DBD::Pg;
use strict;

my ( $db, $table_list, $row_count, $table_cur, $row_cur, $row_rec, $table_rec, $table_name, $relation_size, $relation_cur, $relation_rec );

my $database = $ARGV[0];
my $schema = $ARGV[1];

$db = DBI->connect("DBI:Pg:dbname=$database",'','', {'RaiseError' => 1});
if ( !defined($db) ){
	print STDERR "Connection to database failed\n";
	print STDERR $db->errstr, "\n";
	exit 1;
}

$table_list = << "EOH";
select tablename from pg_tables where schemaname = ?
EOH


$table_cur = $db->prepare($table_list);

$table_cur->execute($schema);

$relation_size = << "EOH";
	set search_path = $schema;
	select pg_size_pretty(pg_relation_size(?)) as size;
EOH

#print $schema, "\n";
printf "%-30s %10s %20s \n", "Table", "Rows", "Size";
print "--------------------------------------------------------------\n";
while ( $table_rec = $table_cur->fetchrow_hashref() ){
	$table_name = $$table_rec{"tablename"};

$row_count = << "EOH";
	set search_path = $schema;
	select count(*) as count from $table_name
EOH

	$row_cur = $db->prepare($row_count);
	$row_cur->execute();
	$row_rec = $row_cur->fetchrow_hashref();
	$relation_cur = $db->prepare($relation_size);
	$relation_cur->execute($table_name);
	$relation_rec = $relation_cur->fetchrow_hashref();
	printf "%-30s %10d %20s \n",$$table_rec{"tablename"},$$row_rec{"count"}, $$relation_rec{"size"};
}
