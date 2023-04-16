import yaml
import subprocess as sub
import os
import queue
import threading
import argparse
import shutil
import concurrent.futures

class Agent:
    def __init__(self, ws, jobs, **kwargs):
        self.boss = Boss(ws)
        if "rerun_status" in kwargs:
            self.boss.set_rerun_status(kwargs["rerun_status"])
        self.ws = ws
        self.jobs = jobs
        self.load_jobs(jobs.keys())
    def load_jobs(self, selected_job_names):
        print(selected_job_names)
        for job_name, job_data in self.jobs.items():
            if job_name not in selected_job_names:
                continue
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
        df_ori = pd.DataFrame(agent.get_result())

        def fix_df(df):
            df = df[["job_name", "status", "envs"]]
            df["status"] = df["status"].apply(status_color)
            df["envs"] = df["envs"].apply(env_str)
            return df

        def reload_jobs():
            df = pd.DataFrame(agent.get_result())
            return fix_df(df)
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
        def gui_run(df, max_workers):
            agent.load_jobs(list(df["job_name"]))
            agent.run(max_workers)
            return "Done"
        def get_df(text_filter):
            df = df_ori.query(text_filter)
            print(text_filter, df)
            return fix_df(df)
        with gr.Blocks() as demo:
            btn2 = gr.Button("Refresh Status Table")
            df_filter = gr.Textbox(label="df filter", info="input filter for the following table for check and run")
            gdf = gr.DataFrame(reload_jobs(), interactive=False, datatype=["str", "markdown", "str", "str"], wrap=True)
            with gr.Box():
                gr.Markdown("Job Details")
                with gr.Row():
                    detail = gr.Code(language="yaml")
                    console_log = gr.Code(language="shell")
            with gr.Box():
                gr.Markdown("Run Controller")
                with gr.Row():
                    sld = gr.Slider(0, 1000, value=1, step=1, label="Max Workers")
                    btn = gr.Button("Start")
                    lb = gr.Label("Ready To Start")
            df_filter.submit(get_df, inputs=[df_filter], outputs=[gdf])
            btn.click(gui_run, inputs=[gdf, sld], outputs=[lb]).then(reload_jobs, outputs=[gdf])
            btn2.click(reload_jobs, outputs=[gdf])
            def gdf_select(evt: gr.SelectData):
                row = evt.index[0]
                col = evt.index[1]
                val = evt.value
                if col == 3:
                    sub.run(f"gvim {val}", shell=True)
                if col == 0:
                    report = agent.boss.workers[val].job_report()
                    yaml_out = yaml.dump(report, sort_keys=False, default_flow_style=False)
                    console_log_file = report["console_log"]
                    job_log = ""
                    if os.path.isfile(console_log_file):
                        with open(report["console_log"]) as fp:
                            job_log = fp.read()
                    return yaml_out, job_log
                return "", ""
            gdf.select(gdf_select, outputs=[detail, console_log])
            demo.load(reload_jobs, outputs=[gdf])
        demo.launch(inbrowser=True)

class Boss:
    def __init__(self, ws):
        self.workers = {}
        self.todo_workers = []
        self.ws = ws
        self.rerun_status = ["ERROR"]
    def reset(self):
        self.workers = {}
        self.todo_workers = {}
    def hire_worker(self, worker):
        self.workers[worker.job_name] = worker
        self.todo_workers.append(worker)
    def get_result(self):
        result = []
        for w in self.workers.values():
            result.append(w.job_report())
        return result
    def run_project(self, max_concurrent):
        while len(self.todo_workers) > 0:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
                futures = []
                for w in self.todo_workers[:]:
                    if w.dep is not None:
                        if self.workers[w.dep].status != "DONE":
                            continue
                    self.todo_workers.remove(w)
                    if w.status not in self.rerun_status + [""]:
                        continue
                    w.setup_cwd()
                    future = executor.submit(w.act)
                    futures.append(future)
                [future.result() for future in futures]
    def set_rerun_status(self, status_list):
        self.rerun_status = status_list

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
        self.yaml = self.cwd + "/se_job.yaml"
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
        with open(self.yaml, "w") as fp:
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
