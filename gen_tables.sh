#!/bin/bash

python exp_tpcc_generate_tables.py --tabledir=/mnt/amida_02/tpcc/1W-tables/ --warehouses=1 --duration=100000 --port=9091
python exp_tpcc_generate_tables.py --tabledir=/mnt/amida_02/tpcc/10W-tables/ --warehouses=10 --duration=100000 --port=9092
python exp_tpcc_generate_tables.py --tabledir=/mnt/amida_02/tpcc/20W-tables/ --warehouses=20 --duration=100000 --port=9093
python exp_tpcc_generate_tables.py --tabledir=/mnt/amida_02/tpcc/50W-tables/ --warehouses=50 --duration=100000 --port=9094
python exp_tpcc_generate_tables.py --tabledir=/mnt/amida_02/tpcc/100W-tables/ --warehouses=100 --duration=100000 --port=9095
