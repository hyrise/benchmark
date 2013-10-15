import multiprocessing
import os
import shutil
import subprocess

from settings import Settings

class Build:

    def __init__(self, settings):
        if not isinstance(settings, Settings):
            raise Exception("You must provide either a valid settings file name or Settings object instance")
        self._settings     = settings
        self._dirSource    = os.path.join(os.getcwd(), "hyrise")
        self._dirBuild     = os.path.join(self._dirSource, "build")
        self._dirBuildBak  = os.path.join(self._dirSource, "build.bak")
        self._dirResult    = os.path.join(os.getcwd(), "builds/" + settings.getName())
        self._dirLog       = os.path.join(os.getcwd(), "logs/" + settings.getName())
        self._logfile      = os.path.join(self._dirLog, "build_log.txt")
        self._settingsfile = os.path.join(self._dirResult, "settings.mk")
        self._prepare()

    def makeAll(self):
        self.link()
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = self._dirResult + ":/usr/local/lib64"
        process = subprocess.Popen("make -j %s" % multiprocessing.cpu_count(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd=self._dirSource, env=env)
        (stdout, stderr) = process.communicate()
        open(self._logfile, "w").write(stderr)
        if process.returncode != 0:
            raise Exception("build '%s' failed with return code %s, check %s for details" % (self._settings.getName(), process.returncode, self._logfile))
        self.unlink()

    def makeClean(self):
        self.link()
        process = subprocess.Popen("make clean", stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd=self._dirSource)
        (stdout, stderr) = process.communicate()
        self.unlink()

    def link(self):
        if os.path.isdir(self._dirBuild) and not os.path.islink(self._dirBuild):
            os.rename(self._dirBuild, self._dirBuildBak)
        elif os.path.islink(self._dirBuild):
            os.remove(self._dirBuild)
        os.symlink(self._dirResult, self._dirBuild)
        shutil.copy2(self._settingsfile, os.path.join(self._dirSource, "settings.mk"))

    def unlink(self):
        if os.path.islink(self._dirBuild):
            os.remove(self._dirBuild)
        os.rename(self._dirBuildBak, self._dirBuild)
        os.remove(os.path.join(self._dirSource, "settings.mk"))

    def _prepare(self):
        src = ""
        if os.path.isdir(self._dirBuild) and not os.path.islink(self._dirBuild):
            src = self._dirBuild
        elif os.path.isdir(self._dirBuildBak):
            src = self._dirBuild
        else:
            raise Exception("No valid build directory found in %s" % self._dirSource)

        if not os.path.exists(self._dirResult):
            shutil.copytree(src, self._dirResult, symlinks=False, ignore=None)
            self._settings.writeToFile(self._settingsfile)
            self.makeClean()
        if not os.path.exists(self._settingsfile) or not self._settings.isSameAs(self._settingsfile):
            self._settings.writeToFile(self._settingsfile)

        if not os.path.exists(self._dirLog):
            os.makedirs(self._dirLog)
