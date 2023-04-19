import yaml
import subprocess as sub
import os
import queue
import threading
import argparse
import shutil
import concurrent.futures
from datetime import datetime

class Agent:
    def __init__(self, ws, jobs):
        self.boss = Boss()
        self.ws = ws
        self.jobs = jobs
        self.load_jobs_by_names(jobs.keys())
    def load_jobs_by_names(self, selected_job_names):
        self.boss.reset()
        for job_name, job_data in self.jobs.items():
            if job_name not in selected_job_names:
                continue
            w = Worker(job_name, job_data, self.ws)
            self.boss.hire_worker(w)
    def run(self, max_concurrent):
        ws = self.ws
        if not os.path.exists(ws):
            os.makedirs(ws)
        with open(f"{ws}/se_jobs.yaml", "w") as fp:
            yaml.dump(self.jobs, fp, sort_keys=False, default_flow_style=False)
        return self.boss.run(max_concurrent)
    def get_result_table(self):
        return self.boss.get_result_table()
    def get_worker_report(self, job_name):
        return self.boss.get_worker_report(job_name)
    def dump_csv(self, output_file):
        import pandas as pd
        df = pd.DataFrame(self.get_result_table())
        df.to_csv(output_file)
    def launch_gui(self):
        GUI(self)


class GUI:
    def __init__(self, agent):
        import pandas as pd
        import gradio as gr

        def get_jobs_df(text_filter=""):
            df = pd.DataFrame(agent.get_result_table())
            df["status"] = df["status"].apply(status_color)
            df["job_start_time"] = df["job_start_time"].apply(pre)
            show_col = ["job_name", "status", "job_start_time", "job_duration"] + [col for col in df.columns if 'env/' in col]
            if text_filter.strip() != "":
                df = df.query(text_filter)
            return df[show_col]
        def pre(val):
            return f"<pre>{val}</pre>"
        def status_color(val):
            color_map = {
                    "RUNNING": "blue",
                    "ERROR": "red",
                    "DONE": "green",
            }
            color = color_map.get(val, "gray")
            return f"<span style='color:{color}'>{val}</span>"
        def gui_run(df, max_workers):
            agent.load_jobs_by_names(list(df["job_name"]))
            return agent.run(max_workers)
        with gr.Blocks() as demo:
            gr_refresh_btn = gr.Button("Refresh Status Table")
            gr_filter_textbox = gr.Textbox(label="df filter", info="input filter for the following table for check and run")
            gr_df = gr.DataFrame(interactive=False, wrap=True)
            gr_df.datatype = "markdown"
            with gr.Box():
                gr.Markdown("Job Details")
                with gr.Row():
                    gr_detail_code = gr.Code(language="yaml")
                    gr_console_code = gr.Code(language="shell")
            with gr.Box():
                gr.Markdown("Run Controller")
                with gr.Row():
                    gr_nworks_slider = gr.Slider(0, 1000, value=1, step=1, label="Max Workers")
                    gr_start_btn = gr.Button("Start")
                    gr_status_label = gr.Label("Ready To Start")
            gr_filter_textbox.submit(get_jobs_df, inputs=[gr_filter_textbox], outputs=[gr_df])
            gr_start_btn.click(gui_run, inputs=[gr_df, gr_nworks_slider], outputs=[gr_status_label]).then(
                    get_jobs_df, outputs=[gr_df], inputs=[gr_filter_textbox]
            )
            gr_refresh_btn.click(get_jobs_df, outputs=[gr_df], inputs=[gr_filter_textbox])
            def gr_df_select(evt: gr.SelectData):
                row = evt.index[0]
                col = evt.index[1]
                val = evt.value
                if col == 0:
                    report = agent.get_worker_report(val)
                    yaml_out = yaml.dump(report, sort_keys=False, default_flow_style=False)
                    console_log_file = report["console_log"]
                    job_log = ""
                    if os.path.isfile(console_log_file):
                        with open(report["console_log"]) as fp:
                            job_log = fp.read()
                    return yaml_out, job_log
                return "", ""
            gr_df.select(gr_df_select, outputs=[gr_detail_code, gr_console_code])
            demo.load(get_jobs_df, outputs=[gr_df], inputs=[gr_filter_textbox])
        demo.launch(inbrowser=True)

class Boss:
    def __init__(self):
        self.workers = {}
        self.todo_workers = []
    def reset(self):
        self.workers = {}
        self.todo_workers = []
    def hire_worker(self, worker):
        self.workers[worker.job_name] = worker
        self.todo_workers.append(worker)
    def get_result_table(self):
        result = []
        for w in self.workers.values():
            result.append(w.job_table())
        return result
    def get_worker_report(self, job_name):
        return self.workers[job_name].job_table()
    def run(self, max_concurrent):
        while len(self.todo_workers) > 0:
            complete_jobs = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
                futures = []
                for w in self.todo_workers[:]:
                    if w.dep is not None:
                        if self.workers[w.dep].status != "DONE":
                            continue
                    self.todo_workers.remove(w)
                    complete_jobs += 1
                    if w.status == "DONE":
                        continue
                    w.setup_cwd()
                    future = executor.submit(w.act)
                    futures.append(future)
                [future.result() for future in futures]
            if complete_jobs == 0:
                print("Can't complete any job due to dependency error")
                return "Dependency Errors"
        return "Done"

class Worker:
    def __init__(self, job_name, job_data, ws):
        self.job_name = job_name
        self.job_data = job_data
        self.dep = job_data.get("dep", None)
        self.envs = job_data.get("envs", {})
        if not isinstance(self.envs, dict):
            raise ValueError(job_name + " env is not dict: ", self.envs)
        dep_path = f"{os.path.realpath(ws)}/{self.dep}"
        cmds = job_data.get("cmds", [])
        if not isinstance(cmds, list):
            raise ValueError(job_name + " cmds is not list: ", cmds)
        cmds = [c.replace("@DEP", dep_path) for c in cmds]
        cmds = [c.replace("@WD", os.getcwd()) for c in cmds]
        self.cmds = cmds
        if len(self.cmds) == 0:
            raise ValueError(f"{job_name} cmds is empty")
        self.failed_cmd = ""
        self.cwd = ws + "/" + self.job_name
        self.log_path = self.cwd + "/se_console.log"
        self.status = self.get_status()
        self.yaml = self.cwd + "/se_job.yaml"
        self.user_results = self.get_user_results()
        self.get_job_time()
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
        self.dump_job_yaml()
    def get_status(self):
        status_map = {
            "SE_STATUS@DONE": "DONE",
            "SE_STATUS@ERROR": "ERROR",
            "SE_STATUS@RUNNING": "RUNNING",
            "SE_STATUS@WAITING": "WAITING",
        }
        for k, v in status_map.items():
            f = self.cwd + "/" + k
            if os.path.isfile(f):
                return v
        return ""
    def update_status(self, status):
        self.status = status
        sub.run(f"rm -f SE_STATUS*;touch SE_STATUS@{self.status}", shell=True, cwd=self.cwd)
    def get_job_time(self):
        if os.path.isfile(self.yaml):
            with open(self.yaml) as fp:
                yd = yaml.safe_load(fp)
                self.job_data["job_duration"] = yd[self.job_name].get("job_duration", "0")
                self.job_data["job_start_time"] = yd[self.job_name].get("job_start_time", "0")
    def dump_job_yaml(self):
        with open(self.yaml, "w") as fp:
            yd = {self.job_name: self.job_data}
            yaml.dump(yd, fp, sort_keys=False, default_flow_style=False)
    def act(self):
        envs = {k: str(v) for k, v in self.envs.items()}
        all_env = {**os.environ, **envs}
        self.update_status("RUNNING")
        job_start_time = datetime.now().replace(microsecond=0)
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
        self.user_results = self.get_user_results()
        job_end_time = datetime.now().replace(microsecond=0)
        job_duration = str(job_end_time - job_start_time)
        self.job_data["job_duration"] = job_duration
        self.job_data["job_start_time"] = job_start_time
        self.dump_job_yaml()
        self.update_status("DONE")
    def get_user_results(self):
        user_result_file = f"{self.cwd}/se_user_result.yaml"
        user_results = {}
        if os.path.exists(user_result_file):
            with open(user_result_file) as fp:
                try:
                    user_results = yaml.safe_load(fp)
                    if not isinstance(user_results, dict):
                        user_results = {}
                        print(self.job_name, " user result yaml is not dict")
                except yaml.YAMLError as e:
                    user_results = {}
                    print(self.job_name, e)
        return user_results
    
    def job_table(self):
        result = self.job_report()
        for target in ["envs", "results"]:
            kv = result.pop(target)
            for k, v in kv.items():
                t = target[:-1]
                result[f"{t}/{k}"] = v
        return result
 
    def job_report(self):
        result = {
            "job_name": self.job_name,
            "status": self.status,
            "cmds": self.cmds,
            "failed_cmd": self.failed_cmd,
            "cwd": self.cwd,
            "console_log": self.log_path,
            "envs": self.envs,
            "results": self.user_results,
            "job_start_time": self.job_data.get("job_start_time", "0"),
            "job_duration": self.job_data.get("job_duration", "0"),
        }
        return result
