#!/bin/bash


BENCHS="etl_1.py etl_2.py etl_3.py etl_co.py etl_join.py"
BATCH_SIZES="1 1000"
RUNS=10


dir=`dirname $0`

for bench in $BENCHS
do
	for batch_size in $BATCH_SIZES
	do
		for run in `seq 1 $RUNS`
		do
			# Some benchs write to stdout, while time reports via stderr
			time=`(/usr/bin/time --format="%e" $dir/$bench $batch_size > /dev/null) 2> /dev/stdout` 	
			echo "$bench $batch_size $time"
			mongo --eval "db.dropDatabase()" localhost/etlpro > /dev/null
		done
	done
done

