# -*- coding: utf-8 -*-

import os
import sys
import atexit
import signal


class Daemon(object):

    pidfile = None
    root = '/'

    def __init__(self, pidfile='/tmp/daemon.pid', root='/'):
        self.pidfile = pidfile
        self.root = root

    def background(self):
        # first fork
        # root process waits for the child in order not to have zombies in the system
        pid = os.fork()
        if pid > 0:
            # parent - root process wait for first child and exits
            os.wait()
            sys.exit(0)

        # first child
        # setup own environment
        os.chdir(self.root)
        os.umask(0)
        os.setsid()

        # second fork
        # first-fork child produces the real worker process and exits
        # first-fork child is being waited now by root process
        pid = os.fork()
        if pid > 0:
            sys.exit(0)

        # worker
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        # handle pid file
        atexit.register(self.delete_pidfile)
        self.write_pidfile()

        # handle streams
        self.redirect_std_streams()

    def shutdown(self):
        self.delete_pidfile()
        sys.exit(0)

    def redirect_std_streams(self):
        sys.stdout.flush()
        sys.stderr.flush()

        stdin = open(os.devnull, 'r')
        stdout = open(os.devnull, 'a+')
        stderr = open(os.devnull, 'a+')

        os.dup2(stdin.fileno(), sys.stdin.fileno())
        os.dup2(stdout.fileno(), sys.stdout.fileno())
        os.dup2(stderr.fileno(), sys.stderr.fileno())

    def write_pidfile(self):
        pid = str(os.getpid())
        with open(self.pidfile, 'w+') as f:
            f.write(pid)

    def delete_pidfile(self):
        try:
            os.remove(self.pidfile)
        except:
            pass

    def get_pid(self):
        try:
            with open(self.pidfile, 'r') as pf:
                pid = int(pf.read().strip())
        except:
            pid = None
        return pid

    def start(self):
        pid = self.get_pid()
        if pid:
            return False
        self.background()
        self.run()

    def stop(self, sig=signal.SIGTERM):
        pid = self.get_pid()
        if not pid:
            return False
        try:
            os.kill(pid, sig)
        except OSError as err:
            estr = str(err.args)
            if estr.find("No such process") > 0:
                self.delete_pidfile()

    def restart(self):
        self.stop()
        self.start()

    def run(self):
        pass

