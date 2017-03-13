#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import math
import db_partition_app as Assignment1

RangeQueryOutputFile = "RangeQueryOut.txt"
PointQueryOutputFile = "PointQueryOut.txt"


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):

    try:
        cur = openconnection.cursor()
        cur.execute("SELECT COUNT(*) FROM RangeRatingsMetadata;")
        numberofpartitions = cur.fetchone()[0]

        MinRating = 0.0
        MaxRating = 5.0
        step = (MaxRating - MinRating) / (float)(numberofpartitions)

        if ratingMinValue < 0.0:
            ratingMinValue = 0.0

        if ratingMaxValue > 5.0:
            ratingMaxValue = 5.0

        if ratingMinValue <= ratingMaxValue:

            if ratingMinValue.is_integer() and ratingMinValue != 0.0:
                minRating = ratingMinValue - step
            else:
                minRating = math.floor(ratingMinValue)

            rangepartitionNameList = []
            rangeName = "RangeRatingsPart"

            while minRating <= ratingMaxValue:
                # Figuring out the range partition number corresponding to given min and max values
                if minRating != 5:
                    cur.execute(
                        "SELECT PartitionNum FROM RangeRatingsMetadata WHERE MinRating = %f AND MaxRating = %f ;" % (
                            minRating, minRating + step))
                    partitionNum = cur.fetchone()[0]
                    rangepartitionNameList.append(rangeName + str(partitionNum))
                    minRating = minRating + step
                else:
                    break;

            for rangepartitonName in rangepartitionNameList:
                cur.execute("SELECT * FROM " + rangepartitonName + " WHERE Rating >= " + str(
                    ratingMinValue) + "and Rating <= " + str(
                    ratingMaxValue) + ";")
                rangerows = cur.fetchall()

                with open(RangeQueryOutputFile, 'a') as rangeFile:
                    for row in rangerows:
                        writeableRow = str(rangepartitonName) + "," + str(row[0]) + "," + str(row[1]) + "," + str(
                            row[2])
                        rangeFile.write(writeableRow + '\n')

            robinName = "RoundRobinRatingsPart"
            for i in range(0, numberofpartitions):
                robinPartitionName = robinName + str(i)
                cur.execute(
                    "SELECT * FROM " + robinPartitionName + " WHERE Rating >= " + str(
                        ratingMinValue) + "and Rating <= " + str(
                        ratingMaxValue) + ";")
                roundrobinrows = cur.fetchall()
                with open(RangeQueryOutputFile, 'a') as rangeFile:
                    for row in roundrobinrows:
                        writeableRow = str(robinPartitionName) + "," + str(row[0]) + "," + str(row[1]) + "," + str(
                            row[2])
                        rangeFile.write(writeableRow + '\n')
        else:
            with open(RangeQueryOutputFile, 'a') as rangeFile:
                rangeFile.write(" ")

    except psycopg2.DatabaseError, e:
        Assignment1.deleteTables(ratingsTableName, openconnection)
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        Assignment1.deleteTables(ratingsTableName, openconnection)
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cur:
            cur.close()


def PointQuery(ratingsTableName, ratingValue, openconnection):
    try:
        if ratingValue >= 0.0 and ratingValue <= 5.0:
            cur = openconnection.cursor()
            cur.execute("SELECT COUNT(*) FROM RangeRatingsMetadata;")
            numberofpartitions = cur.fetchone()[0]

            MinRating = 0.0
            MaxRating = 5.0
            step = (MaxRating - MinRating) / (float)(numberofpartitions)

            if ratingValue.is_integer() and ratingValue != 0.0:
                lowerRange = ratingValue - step
                upperRange = ratingValue
            else:
                lowerRange = math.floor(ratingValue)
                upperRange = lowerRange + step

            rangeName = "RangeRatingsPart"

            cur.execute("SELECT PartitionNum FROM RangeRatingsMetadata WHERE MinRating = %f AND MaxRating = %f ;" % (
                lowerRange, upperRange))
            partitionNum = cur.fetchone()[0]

            rangepartitonName = rangeName + str(partitionNum)
            cur.execute("SELECT * FROM " + rangepartitonName + " WHERE Rating = " + str(ratingValue) + ";")
            rangerows = cur.fetchall()

            with open(PointQueryOutputFile, 'a') as pointFile:
                for row in rangerows:
                    writeableRow = str(rangepartitonName) + "," + str(row[0]) + "," + str(row[1]) + "," + str(row[2])
                    pointFile.write(writeableRow + '\n')

            robinName = "RoundRobinRatingsPart"
            for i in range(0, numberofpartitions):
                robinPartitionName = robinName + str(i)
                cur.execute("SELECT * FROM " + robinPartitionName + " WHERE Rating = " + str(ratingValue) + ";")
                roundrobinrows = cur.fetchall()
                with open(PointQueryOutputFile, 'a') as pointFile:
                    for row in roundrobinrows:
                        writeableRow = str(robinPartitionName) + "," + str(row[0]) + "," + str(row[1]) + "," + str(
                            row[2])
                        pointFile.write(writeableRow + '\n')
        else:
            with open(PointQueryOutputFile, 'a') as pointFile:
                pointFile.write(" ")

    except psycopg2.DatabaseError, e:
        Assignment1.deleteTables(ratingsTableName, openconnection)
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        Assignment1.deleteTables(ratingsTableName, openconnection)
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cur:
            cur.close()
