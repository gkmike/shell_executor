import yaml
import subprocess as sub
import os
import queue
import threading
import argparse
import shutil


class Agent:
    def __init__(self, ws, jobs):
        if not os.path.exists(ws):
            os.makedirs(ws)
        self.b = Boss(ws)
        with open(f"{ws}/se_jobs.yaml", "w") as fp:
            yaml.dump(jobs, fp, sort_keys=False, default_flow_style=False)
        for job_name, job_data in jobs.items():
            w = Worker(job_name, job_data)
            self.b.hire_worker(w)
    def run(self, max_concurrent):
        self.b.start_works(max_concurrent)
    def get_result(self):
        result = []
        for w in self.b.workers:
            result.append(w.job_report())
        return result
    def dump_csv(self, output_file):
        import pandas
        df = pandas.DataFrame(self.get_result())
        df.to_csv(output_file)


class Boss:
    def __init__(self, ws):
        self.workers = [] 
        self.worker_queue = queue.Queue()
        self.ws = ws
    def hire_worker(self, worker):
        worker.set_cwd(self.ws)
        self.workers.append(worker)
        self.worker_queue.put(worker)
    def start_works(self, max_concurrent):
        all_threads = []
        for i in range(max_concurrent):
            th = threading.Thread(target=self.send_worker)
            th.setDaemon(True)
            all_threads.append(th)
            th.start()
        for th in all_threads:
            th.join()
    def send_worker(self):
        while self.worker_queue.qsize() > 0:
            worker = self.worker_queue.get()
            worker.act()

class Worker:
    def __init__(self, job_name, job_data):
        self.job_name = job_name
        self.job_data = job_data
        self.envs = job_data.get("envs", {})
        self.cmds = job_data.get("cmds", [])
        if len(self.cmds) == 0:
            raise ValueError(f"{job_name} cmds is empty")
        self.done_cmds = []
        self.status = ""
        self.cwd = ""
        self.log_path = ""
        self.failed_cmd = ""
    def set_cwd(self, ws):
        self.status = "INIT"
        self.cwd = ws + "/" + self.job_name
        self.log_path = self.cwd + "/se_console.log"
        if os.path.exists(self.cwd):
            shutil.rmtree(self.cwd)
        os.makedirs(self.cwd)
        rerun_sh = self.cwd + "/rerun.sh"
        with open(rerun_sh, "w") as fp:
            fp.write("set -e -x \n")
            for k, v in self.envs.items():
                fp.write(f"export {k}={v} \n")
            for c in self.cmds:
                fp.write(f"{c} \n")
        os.chmod(rerun_sh, 0o755)
        cmd_yaml = self.cwd + "/se_job.yaml"
        with open(cmd_yaml, "w") as fp:
            yd = {self.job_name: self.job_data}
            yaml.dump(yd, fp, sort_keys=False, default_flow_style=False)
        
    def act(self):
        envs = {k: str(v) for k, v in self.envs.items()}
        all_env = {**os.environ, **envs}
        self.status = "RUNNING"
        with open(self.log_path, "w") as fp:
            for c in self.cmds:
                fp.write(f"++ {c}\n")
                fp.flush()
                r = sub.run(c, shell=True, stdout=fp, stderr=fp, cwd=self.cwd, env=all_env)
                ret_code = r.returncode
                if ret_code != 0:
                    self.failed_cmd = c
                    self.status = "ERROR"
                    return
                self.done_cmds.append(c)
        self.status = "DONE"
    def job_report(self):
        result = {
            "job_name": self.job_name,
            "status": self.status,
            "envs": self.envs,
            "cmds": self.cmds,
            "done_cmds": self.done_cmds,
            "failed_cmd": self.failed_cmd,
            "cwd": self.cwd,
            "console_log": self.log_path,
        }
        return result
