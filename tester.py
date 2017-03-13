#!/usr/bin/python2.7
#
#Tester
#

import db_partition_app as db_part_app
import query_processor as query_proc

if __name__ == '__main__':
    try:
        #Creating Database ddsassignment2
        print "Creating Database named as ddsassignment2"
        db_part_app.createDB();

        # Getting connection to the database
        print "Getting connection from the ddsassignment2 database"
        con = db_part_app.getOpenConnection();

        # Loading Ratings table
        print "Creating and Loading the ratings table"
        db_part_app.loadRatings('ratings', 'ratings.dat', con);

        # Doing Range Partition
        print "Doing the Range Partitions"
        db_part_app.rangePartition('ratings', 5, con);

        # Doing Round Robin Partition
        print "Doing the Round Robin Partitions"
        db_part_app.roundRobinPartition('ratings', 5, con);

        # Deleting Ratings Table because Point Query and Range Query should not use ratings table instead they should use partitions.
        db_part_app.deleteTables('ratings', con);

        # Calling RangeQuery
        print "Performing Range Query"
        query_proc.RangeQuery('ratings', 1, 2, con);

        # Calling PointQuery
        print "Performing Point Query"
        query_proc.PointQuery('ratings', 0.0, con);
        
        # Deleting All Tables
        db_part_app.deleteTables('all', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
