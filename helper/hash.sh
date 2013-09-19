#!/bin/bash
{
	export LC_ALL=C;
	find $1 -type f \( ! -iname '.*' \)  -not -iwholename '*.git*' -exec wc -c {} \; | sort; echo;
	find $1 -type f \( ! -iname '.*' \)  -not -iwholename '*.git*' -exec md5sum {} + | sort; echo;
	find $1 -not -iwholename '*.git*' -type d | sort;
	find $1 -not -iwholename '*.git*' -type d | sort | md5sum;
} | md5sum
