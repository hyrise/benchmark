from tpcc_parameters import *
import shutil

groupId = "tmp_tpcc_generate"
runId = "gen"
parameters = {"none":None}    
b1 = create_benchmark_none("None", groupId, parameters, kwargs)


b1.benchPrepare()
csv_dir = os.path.join(kwargs["tabledir"], "csv")
bin_dir = os.path.join(kwargs["tabledir"], "bin")

if not os.path.exists(csv_dir):
  print "CSV directory does not exist:", csv_dir
  print "Generating CSV files..."
  b1.generateTables(path = csv_dir)
else:
  print "CSV directory does exist:", csv_dir
  print "We use existing CSV files..."

if os.path.exists(bin_dir):
  print "Bin directory does already exist:", csv_dir
  print "Deleting Bin directory and generating binary files..."
  shutil.rmtree(bin_dir)

b1.createBinaryTableExport( import_path = csv_dir,
                            export_path = bin_dir)
