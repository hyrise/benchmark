import re
import glob
import os

def getlastprefix(prefix):
    path = "results%s" % prefix

    files = glob.glob(os.path.join(os.path.dirname(__file__), "..", path + "*"))
    if len(files) == 0:
        return path + "_1"

    print "found one"
    files.sort()
    path = files[-1]
    match = re.search("_?(\d+)$", path)
    if match:
        current = int(match.groups()[0])
        newpath =  path.replace(match.groups()[0], str(current+1))
        return os.path.basename(newpath)
    else:
        return path + "_1"
