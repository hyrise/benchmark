#!/bin/bash

python exp_tpcc_generate_queries.py --tabledir=/mnt/amida_02/tpcc/1W-tables/ --genCount=1000000 --genFile=/mnt/amida_02/tpcc/queries_gen/1W_neworder_1M.txt --warehouses=1 --duration=100000 --port=9090
python exp_tpcc_generate_queries.py --tabledir=/mnt/amida_02/tpcc/10W-tables/ --genCount=1000000 --genFile=/mnt/amida_02/tpcc/queries_gen/10W_neworder_1M.txt --warehouses=10 --duration=100000 --port=9091
python exp_tpcc_generate_queries.py --tabledir=/mnt/amida_02/tpcc/20W-tables/ --genCount=1000000 --genFile=/mnt/amida_02/tpcc/queries_gen/20W_neworder_1M.txt --warehouses=20 --duration=100000 --port=9092
python exp_tpcc_generate_queries.py --tabledir=/mnt/amida_02/tpcc/50W-tables/ --genCount=1000000 --genFile=/mnt/amida_02/tpcc/queries_gen/50W_neworder_1M.txt --warehouses=50 --duration=100000 --port=9093
python exp_tpcc_generate_queries.py --tabledir=/mnt/amida_02/tpcc/100W-tables/ --genCount=1000000 --genFile=/mnt/amida_02/tpcc/queries_gen/100W_neworder_1M.txt --warehouses=100 --duration=100000 --port=9094



python exp_tpcc_generate_tables.py --tabledir=/mnt/amida_02/tpcc/1W-tables/ --warehouses=1 --duration=100000 --port=9091
python exp_tpcc_generate_tables.py --tabledir=/mnt/amida_02/tpcc/10W-tables/ --warehouses=10 --duration=100000 --port=9092
python exp_tpcc_generate_tables.py --tabledir=/mnt/amida_02/tpcc/20W-tables/ --warehouses=20 --duration=100000 --port=9093
python exp_tpcc_generate_tables.py --tabledir=/mnt/amida_02/tpcc/50W-tables/ --warehouses=50 --duration=100000 --port=9094
python exp_tpcc_generate_tables.py --tabledir=/mnt/amida_02/tpcc/100W-tables/ --warehouses=100 --duration=100000 --port=9095