# NAME

Pg::Reindex - rebuild postgresql indexes concurrently without locking.

[![Build Status](https://travis-ci.org/binary-com/perl-Pg-Reindex.svg?branch=master)](https://travis-ci.org/binary-com/perl-Pg-Reindex)
[![codecov](https://codecov.io/gh/binary-com/perl-Pg-Reindex/branch/master/graph/badge.svg)](https://codecov.io/gh/binary-com/perl-Pg-Reindex)

# VERSION

Version 0.01

# SYNOPSIS

### Use as a module

    use Pg::Reindex qw(prepare rebuild);

    prepare($dbh, \@namespaces, \@tables, \@indexes);
    rebuild($dbh, \%options, $dryrun);

### Run as a perl script

    perl `perldoc -l Pg::Reindex` \
               [--help] \
               [--server=localhost] \
               [--port=5432] \
               [--user=postgres] \
               [--password=PASSWORD] \
               [--table=TABLE] ... \
               [--namespace=NAMESPACE] ... \
               [--index=INDEX] ... \
               [--[no]validate] \
               [--high_txn_lag=BYTES] \
               [--log_txn_lag=BYTES] \
               [--[no]dryrun] \
               [prepare|continue]

# DESCRIPTION

Postgresql indexes should be rebuilt on a regular basis for good performance.
This can be done with the `REINDEX` command, however, building indexes this way
requires an exclusive lock on the table. On the other hand, using
`CREATE INDEX CONCURRENTLY` avoids this lock.

`Pg::Reindex` builds new indexes using `CREATE INDEX CONCURRENTLY`. Then it
starts a transaction for each index in which it drops the old index and
renames the new one.

It handles normal indexes and `PRIMARY KEY`, `FOREIGN KEY` and `UNIQUE`
constraints.

## Streaming replication and throttling

Before creating the next index, the streaming replication lag is checked to
be below a certain limit. If so, nothing special happens and the index is
built.

Otherwise, `rebuild` waits for the replicas to catch up. When the lag
drops under a second limit, the `rebuild` does not immediately continue.
Instead it waits for another 30 seconds and checks the lag every second
within that period. Only if the lag stays below the limit for the whole
time, execution is continued. This grace period is to deal with the fact
that a wal sender process may suddenly disappear and reappear after a
few seconds. Without the grace period the program may encounter a false
drop below the limit and hence continue. For large indexes this adds a
lot of lag.

# USING AS A MODULE

To use Pg::Reindex as a module, first you need to load the 
Pg::Reindex module:

    use Pg::Reindex qw(prepare rebuild);
    use strict;

(The `use strict;` isn't required but is strongly recommended.)

Then you need to ["prepare"](#prepare) the indexes that you want rebuilt.
You can filter by combinations of namespace, tables, and indexes.

    prepare($dbh, \@opt_namespaces,\@opt_tables, \@opt_indexes);

After "preparing" the set of indexes to be rebuilt, then you rebuild them:

    rebuild( $dbh, { ThrottleOn => 10000000, 
        ThrottleOff => 100000, Validate => 1 }, $opt_dryrun);

## SUBROUTINES/METHODS

### prepare

`prepare` determines the list of indexes that would be re-indexed, and 
sets up the data structures used by `rebuild`. `prepare` must be called
before `rebuild` is called.

`prepare` creates a new schema named `reindex` with 2 tables, 
`worklist` and `log`. `Worklist` is created as `UNLOGGED`
table. `prepare` saves information on all indexes that need to be rebuilt
to `worklist`. The information in `worklist` is used by `rebuild`.

- $dbh

    DBI database handle to the database whose indexes are to be reindexed.

- \\@namespaces

    Rebuild only indexes in the `namespaces`. If `namespaces` is empty, 
    indexes in all namespaces except the following are considered: 
    those beginning with `pg_`, in `information_schema`i, or are 
    `sequences` namespaces.

- \\@tables

    Rebuild only indexes that belong to the specified tables.

- \\@indexes

    List of indexes to reindex.

If `tables`, `namespaces` and `indexes` are given simultaneously,
only indexes satisfying all conditions are considered.

### rebuild

- $dbh

    DBI database handle to the database whose indexes are to be reindexed.

- \\%options

        ThrottleOn  
        ThrottleOff 
        Validate    
        

- $dryrun

# USING AS A PERL SCRIPT

To use Pg::Reindex as a perl script you need to have perl run it. The command
below would do that by using `perldoc` to determine `Pg::Reindex`'s location.

    perl `perldoc -l Pg::Reindex` \
               [--help] \
               [--server=localhost] \
               [--port=5432] \
               [--user=postgres] \
               [--password=PASSWORD] \
               [--table=TABLE] ... \
               [--namespace=NAMESPACE] ... \
               [--index=INDEX] ... \
               [--[no]validate] \
               [--high_txn_lag=BYTES] \
               [--log_txn_lag=BYTES] \
               [--[no]dryrun] \
               [prepare|continue]

## OPTIONS

Options can be abbreviated.

- --server

    Hostname / IP address or directory path to use to connect to the Postgres
    server. If you want to use a local UNIX domain socket, specify the socket
    directory path.

    Default: localhost

- --port

    The port to connect to.

    Default: 5432

- --user

    The user.

    Default: postgres

- --password

    a file name or open file descriptor where to read the password from.
    If the parameter value consists of only digits, it's evaluated as file
    descriptor.

    There is no default.

    A convenient way to specify the password on the BASH command line is

        reindex.pl --password=3 3<<<my_secret

    That way the password appears in `.bash_history`. But that file is
    usually only readable to the owner.

- --table

    Reindex only indexes that belong to the specified table.

    This option can be given multiple times.

    If `--table`, `--namespace` and `--index` are given simultaneously,
    only indexes satisfying all conditions are considered.

- --namespace

    Without this option only namespaces are considered that are not in
    beginning with `pg_`. Also `information_schema` or `sequences`
    namespaces are omitted.

    If `--table`, `--namespace` and `--index` are given simultaneously,
    only indexes satisfying all conditions are considered.

- --index

    If `--table`, `--namespace` and `--index` are given simultaneously,
    only indexes satisfying all conditions are considered.

- --\[no\]validate

    validate `FOREIGN KEY` constraints or leave them `NOT VALID`. Default
    it to validate.

- --\[no\]dryrun

    don't modify the database but print the essential SQL statements.

- --high-txn-lag

    the upper limit streaming replicas may lag behind in bytes.

    Default is 10,000,000.

- --low-txn-lag

    the lower limit in bytes when execution may be continued after it has been
    interrupted due to exceeding `high_txn_lag`.

    Default is 100,000

- --help

    print this help

# AUTHOR

BINARY, `<binary at cpan.org>`

# BUGS

Please report any bugs or feature requests to `bug-pg-reindex at rt.cpan.org`, or through
the web interface at [http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Pg-Reindex](http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Pg-Reindex).  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

# LICENSE AND COPYRIGHT

Copyright 2015 Binary Ltd.

This program is free software; you can redistribute it and/or modify it
under the terms of the the Artistic License (2.0). You may obtain a
copy of the full license at:

[http://www.perlfoundation.org/artistic\_license\_2\_0](http://www.perlfoundation.org/artistic_license_2_0)

Any use, modification, and distribution of the Standard or Modified
Versions is governed by this Artistic License. By using, modifying or
distributing the Package, you accept this license. Do not use, modify,
or distribute the Package, if you do not accept this license.

If your Modified Version has been derived from a Modified Version made
by someone other than you, you are nevertheless required to ensure that
your Modified Version complies with the requirements of this license.

This license does not grant you the right to use any trademark, service
mark, tradename, or logo of the Copyright Holder.

This license includes the non-exclusive, worldwide, free-of-charge
patent license to make, have made, use, offer to sell, sell, import and
otherwise transfer the Package with respect to any patent claims
licensable by the Copyright Holder that are necessarily infringed by the
Package. If you institute patent litigation (including a cross-claim or
counterclaim) against any party alleging that the Package constitutes
direct or contributory patent infringement, then this Artistic License
to you shall terminate on the date that such litigation is filed.

Disclaimer of Warranty: THE PACKAGE IS PROVIDED BY THE COPYRIGHT HOLDER
AND CONTRIBUTORS "AS IS' AND WITHOUT ANY EXPRESS OR IMPLIED WARRANTIES.
THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE, OR NON-INFRINGEMENT ARE DISCLAIMED TO THE EXTENT PERMITTED BY
YOUR LOCAL LAW. UNLESS REQUIRED BY LAW, NO COPYRIGHT HOLDER OR
CONTRIBUTOR WILL BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, OR
CONSEQUENTIAL DAMAGES ARISING IN ANY WAY OUT OF THE USE OF THE PACKAGE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
