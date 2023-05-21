import argparse
import shell_executor as se
import yaml


class rt_cmd:
    def __init__(self):
        self.cmds = []
    def add(self, c):
        self.cmds.append(c)

class rt_env:
    def __init__(self):
        self.envs = {}
    def set(self, k, v):
        self.envs[k] = v

class rt_files:
    def __init__(self):
        self.links = set()
        self.copys = set()

class rt_job:
    def __init__(self, name):
        self.name = name
        self.file = rt_files() 
        self.env = rt_env() 
        self.cmd = rt_cmd()
    def get_cwd(self):
        return "@DEP" 

class rt_test:
    def __init__(self, name):
        self.name = name
        self.jobs = []
    def create_job(self, name):
        j = rt_job(name)
        self.jobs.append(j)
        return j

class regression_test:
    def __init__(self, ws_dir):
        self.ws_dir = ws_dir
        self.tests = []
    def create_test(self, name):
        t = rt_test(name)
        self.tests.append(t)
        return t
    def process(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-w', '--workers', type=int, default=2, help='max concurrent workers(thread), default=2')
        parser.add_argument('-l', '--list', action="store_true", help='list job only')
        parser.add_argument('-g', '--gui', action="store_true", help='show gui')
        args = parser.parse_args()
        jobs = {}
        for t in self.tests:
            last_job_name = None
            for j in t.jobs:
                job_name = t.name + "_" + j.name
                job_data = {}
                cmds = []
                for l in j.file.links:
                    if not (l.startswith("/") or l.startswith("@")):
                        l = "@WD/" + l
                    cmds.append(f"ln -s {l} . || :")
                for c in j.file.copys:
                    if not (c.startswith("/") or c.startswith("@")):
                        c = "@WD/" + c
                    cmds.append(f"cp -rf {c} . || :")
                cmds.extend(j.cmd.cmds)
                job_data["cmds"] = cmds
                job_data["envs"] = j.env.envs
                if last_job_name is not None:
                    job_data["dep"] = last_job_name
                jobs[job_name] = job_data
                last_job_name = job_name
                
        a = se.Agent(self.ws_dir, jobs)
        if args.gui:
            a.launch_gui()
            return
        if not args.list:
            a.run(args.workers)
        a.dump_csv("se_result.csv")
        print("see se_result.csv")
