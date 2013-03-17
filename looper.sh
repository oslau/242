#!/bin/sh

for i in ${*:2}
do
	sed -n ''$i'p' $1
done
