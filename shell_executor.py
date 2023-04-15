import yaml
import subprocess as sub
import os
import queue
import threading
import argparse
import shutil

class Agent:
    def __init__(self, ws, jobs):
        self.boss = Boss(ws)
        self.ws = ws
        self.jobs = None
        self.load_jobs(jobs)
    def load_jobs(self, jobs):
        self.jobs = jobs
        for job_name, job_data in jobs.items():
            w = Worker(job_name, job_data, self.ws)
            self.boss.hire_worker(w)
    def run(self, max_concurrent):
        ws = self.boss.ws
        if not os.path.exists(ws):
            os.makedirs(ws)
        with open(f"{ws}/se_jobs.yaml", "w") as fp:
            yaml.dump(self.jobs, fp, sort_keys=False, default_flow_style=False)
        self.boss.run_project(max_concurrent)
    def get_result(self):
        return self.boss.get_result()
    def dump_csv(self, output_file):
        import pandas as pd
        df = pd.DataFrame(self.get_result())
        df.to_csv(output_file)
    def launch_gui(self):
        GUI(self)


class GUI:
    def __init__(self, agent):
        import pandas as pd
        import gradio as gr
        def reload_jobs():
            df = pd.DataFrame(agent.get_result())
            df = df[["job_name", "status", "envs", "console_log"]]
            df["status"] = df["status"].apply(status_color)
            df["envs"] = df["envs"].apply(env_str)
            return df
        def status_color(val):
            color_map = {
                    "RUNNING": "blue",
                    "ERROR": "red",
                    "DONE": "green",
            }
            color = color_map.get(val, "gray")
            return f"<span style='color:{color}'>{val}</span>"
        def env_str(d):
            ret = ""
            for k,v in d.items():
                ret += f"{k}={v} "
            return ret
        def gui_run(x):
            agent.run(1)
            return "done"
        with gr.Blocks(theme="default") as demo:
            with gr.Row():
                btn = gr.Button("run")
                btn2 = gr.Button("reload")
            df = reload_jobs()
            gdf = gr.DataFrame(df, interactive=False, datatype=["markdown", "markdown", "markdown", "str"], wrap=True)
            with gr.Box():
                gr.Markdown("Status")
                lb = gr.Label("")
            btn.click(gui_run, inputs=[gdf], outputs=[lb]).then(reload_jobs, outputs=[gdf])
            btn2.click(reload_jobs, outputs=[gdf])
            def gdf_select(df, evt: gr.SelectData):
                row = evt.index[0]
                col = evt.index[1]
                val = evt.value
                print(val)
                if col == 3:
                    sub.run(f"gvim {val}", shell=True)
            gdf.select(gdf_select, inputs=[gdf])
            demo.load(reload_jobs, outputs=[gdf])
        demo.launch(inbrowser=True)

class Boss:
    def __init__(self, ws):
        self.workers = {}
        self.todo_workers = []
        self.worker_queue = queue.Queue()
        self.ws = ws
        self.force_rerun_status = ["ERROR"]
    def hire_worker(self, worker):
        self.workers[worker.job_name] = worker
        if worker.status in self.force_rerun_status + [""]:
            self.todo_workers.append(worker)
    def get_result(self):
        result = []
        for w in self.workers.values():
            result.append(w.job_report())
        return result
    def run_project(self, max_concurrent):
        while len(self.todo_workers) > 0:
            for w in self.todo_workers:
                if w.dep is not None:
                    if self.workers[w.dep].status != "DONE":
                        continue
                w.setup_cwd()
                self.worker_queue.put(w)
                self.todo_workers.remove(w)
            if self.worker_queue.qsize() > 0:
                self.start_works(max_concurrent)
            else:
                print("Can not dispatch job. Please check the job dependency")
                break
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
    def set_force_rerun_status(self, status_list):
        self.force_rerun_status = status_list

class Worker:
    def __init__(self, job_name, job_data, ws):
        self.job_name = job_name
        self.job_data = job_data
        self.dep = job_data.get("dep", None)
        self.envs = job_data.get("envs", {})
        cmds = job_data.get("cmds", [])
        cmds = [c.replace("@DEP", f"{os.path.realpath(ws)}/{self.dep}") for c in cmds]
        cmds = [c.replace("@WD", os.getcwd()) for c in cmds]
        self.cmds = cmds
        if len(self.cmds) == 0:
            raise ValueError(f"{job_name} cmds is empty")
        self.failed_cmd = ""
        self.cwd = ws + "/" + self.job_name
        self.log_path = self.cwd + "/se_console.log"
        self.status = self.get_status()
    def setup_cwd(self):
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
    def get_status(self):
        status_map = {
            "SE_STATUS@DONE": "DONE",
            "SE_STATUS@ERROR": "ERROR",
            "SE_STATUS@WAITING": "WAITING",
            "SE_STATUS@RUNNING": "RUNNING",
        }
        for k, v in status_map.items():
            f = self.cwd + "/" + k
            if os.path.isfile(f):
                return v
        return ""
    def update_status(self, status):
        self.status = status
        sub.run(f"rm -f SE_STATUS*;touch SE_STATUS@{self.status}", shell=True, cwd=self.cwd)
    def act(self):
        envs = {k: str(v) for k, v in self.envs.items()}
        all_env = {**os.environ, **envs}
        self.update_status("RUNNING")
        with open(self.log_path, "w") as fp:
            for c in self.cmds:
                fp.write(f"++ {c}\n")
                fp.flush()
                r = sub.run(c, shell=True, stdout=fp, stderr=fp, cwd=self.cwd, env=all_env)
                ret_code = r.returncode
                if ret_code != 0:
                    self.failed_cmd = c
                    self.update_status("ERROR")
                    return
        self.update_status("DONE")
    def job_report(self):
        result = {
            "job_name": self.job_name,
            "status": self.status,
            "envs": self.envs,
            "cmds": self.cmds,
            "failed_cmd": self.failed_cmd,
            "cwd": self.cwd,
            "console_log": self.log_path,
        }
        return result
