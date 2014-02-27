import multiprocessing
import os
import shutil
import subprocess

from settings import Settings

class Build:

    def __init__(self, settings, ssh=None, remotePath=None):
        if not isinstance(settings, Settings):
            raise Exception("You must provide either a valid settings file name or Settings object instance")
        self._settings           = settings
        self._dirSource          = os.path.join(os.getcwd(), "hyrise")
        self._dirBuild           = os.path.join(self._dirSource, "build")
        self._dirBuildBak        = os.path.join(self._dirSource, "build.bak")
        self._dirHiddenBuild     = os.path.join(self._dirSource, ".build")
        self._dirHiddenBuildBak  = os.path.join(self._dirSource, ".build.bak")
        self._dirResult          = os.path.join(os.getcwd(), "builds/" + settings.getName())
        self._dirLog             = os.path.join(os.getcwd(), "logs/" + settings.getName())
        self._logfile            = os.path.join(self._dirLog, "build_log.txt")
        self._settingsfile       = os.path.join(self._dirResult, "settings.mk")
        self._ssh                = ssh
        self._remotePath         = remotePath
        self._prepare()

    def makeAll(self):
        self.link()
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = self._dirResult + ":/usr/local/lib64"
        if self._ssh is None:
            process = subprocess.Popen("make -j %s" % multiprocessing.cpu_count(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd=self._dirSource, env=env)
            (stdout, stderr) = process.communicate()
            open(self._logfile, "w").write(stderr)
            returncode = process.returncode
        else:
            stdin, stdout, stderr = self._ssh.exec_command("cd " + self._remotePath + "/hyrise && make -j")
            returncode = stdout.channel.recv_exit_status()
            if returncode != 0:
                print stderr.readlines()
            stdin.channel.shutdown_write()

        if returncode != 0:
            raise Exception("build '%s' failed with return code %s:\n===\n%s" % (self._settings.getName(), returncode, stderr))
        self.unlink()

    def makeClean(self):
        self.link()
        if self._ssh is None:
            try:
                process = subprocess.Popen("make clean", stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd=self._dirSource)
                (stdout, stderr) = process.communicate()
            except Exception:
                pass
        else:
            self._ssh.exec_command("make clean")
        self.unlink()

    def link(self):
        settingsFile = os.path.join(self._dirSource, "settings.mk")
        if os.path.isdir(self._dirBuild) and not os.path.islink(self._dirBuild):
            os.rename(self._dirBuild, self._dirBuildBak)
        elif os.path.islink(self._dirBuild):
            os.remove(self._dirBuild)
        if os.path.isdir(self._dirHiddenBuild) and not os.path.islink(self._dirHiddenBuild):
            os.rename(self._dirHiddenBuild, self._dirHiddenBuildBak)
        elif os.path.islink(self._dirHiddenBuild):
            os.remove(self._dirHiddenBuild)
        os.symlink(self._dirResult, self._dirBuild)
        os.symlink(os.path.join(self._dirResult, ".build"), self._dirHiddenBuild)
        if os.path.isfile(settingsFile):
            os.rename(settingsFile, settingsFile+".bak")
        shutil.copy2(self._settingsfile, settingsFile)

    def unlink(self):
        settingsFile = os.path.join(self._dirSource, "settings.mk")
        if os.path.islink(self._dirBuild):
            os.remove(self._dirBuild)
        if os.path.islink(self._dirHiddenBuild):
            os.remove(self._dirHiddenBuild)
        if os.path.isdir(self._dirBuildBak):
            os.rename(self._dirBuildBak, self._dirBuild)
        if os.path.isdir(self._dirHiddenBuildBak):
            os.rename(self._dirHiddenBuildBak, self._dirHiddenBuild)
        if os.path.isfile(settingsFile):
            os.remove(settingsFile)
        if os.path.isfile(settingsFile+".bak"):
            os.rename(settingsFile+".bak", settingsFile)

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

        if not os.path.isdir(os.path.join(self._dirResult, ".build")):
            os.mkdir(os.path.join(self._dirResult, ".build"))
        if not os.path.exists(self._dirLog):
            os.makedirs(self._dirLog)
