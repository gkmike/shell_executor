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
        def get_jobs_df_drop(xx):
            df = get_jobs_df(xx)
            job_names = list(df["job_name"].unique())
            return df, gr.Dropdown.update(choices=job_names)
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
            gr_jnames_dropdown = gr.Dropdown(interactive=True, multiselect=True, label="job_names_filters")
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
            gr_refresh_btn.click(get_jobs_df_drop, outputs=[gr_df, gr_jnames_dropdown], inputs=[gr_filter_textbox])
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
        return self.workers[job_name].job_report()
    def run(self, max_concurrent):
        while len(self.todo_workers) > 0:
            complete_jobs = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
                futures = []
                for w in self.todo_workers[:]:
                    if w.dep is not None:
                        if self.workers[w.dep].attr("status") != "DONE":
                            continue
                    self.todo_workers.remove(w)
                    complete_jobs += 1
                    if w.attr("status") == "DONE":
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
        cwd = ws + "/" + job_name
        self.job_name = job_name
        dep = job_data.get("dep", None)
        self.dep = dep
        if self.get_exist_job_data(cwd):
            return
        job_data["job_name"] = job_name
        job_data["status"] = ""
        envs = job_data.get("envs", {})
        job_data["envs"] = envs
        if not isinstance(envs, dict):
            raise ValueError(job_name + " env is not dict: ", envs)
        dep_path = f"{os.path.realpath(ws)}/{dep}"
        cmds = job_data.get("cmds", [])
        if not isinstance(cmds, list):
            raise ValueError(job_name + " cmds is not list: ", cmds)
        if len(cmds) == 0:
            raise ValueError(f"{job_name} cmds is empty")
        new_cmds = []
        for cmd in cmds:
            if "@DEP" in cmd:
                cmd = cmd.replace("@DEP", dep_path)
            if "@WD" in cmd:
                cmd = cmd.replace("@WD", os.getcwd())
            new_cmds.append(cmd)
        job_data["cmds"] = new_cmds
        job_data["failed_cmd"] = ""
        job_data["cwd"] = cwd
        job_data["console_log"] = cwd + "/se_console.log"
        job_data["results"] = {}
        job_data["job_duration"] = ""
        job_data["job_start_time"] = ""
        self.job_data = job_data
    def attr(self, name):
        return self.job_data[name]
        
    def update_status(self, val):
        self.job_data["status"] = val
    def setup_cwd(self):
        cwd = self.job_data["cwd"]
        envs = self.job_data["envs"]
        cmds = self.job_data["cmds"]
        if os.path.exists(cwd):
            shutil.rmtree(cwd)
        os.makedirs(cwd)
        rerun_sh = cwd + "/rerun.sh"
        with open(rerun_sh, "w") as fp:
            fp.write("set -e -x \n")
            for k, v in envs.items():
                fp.write(f"export {k}={v} \n")
            for c in cmds:
                fp.write(f"{c} \n")
        os.chmod(rerun_sh, 0o755)
    def get_exist_job_data(self, cwd):
        job_yaml = cwd + "/se_job.yaml"
        if os.path.isfile(job_yaml):
            with open(job_yaml) as fp:
                self.job_data = yaml.safe_load(fp)
            return True
        else:
            return False
    def dump_job_data(self):
        job_yaml = self.job_data["cwd"] + "/se_job.yaml"
        with open(job_yaml, "w") as fp:
            yaml.dump(self.job_data, fp, sort_keys=False, default_flow_style=False)
    def act(self):
        cwd = self.job_data["cwd"]
        envs = self.job_data["envs"]
        cmds = self.job_data["cmds"]
        log_path = self.job_data["console_log"]
        envs = {k: str(v) for k, v in envs.items()}
        all_env = {**os.environ, **envs}
        self.update_status("RUNNING")
        job_start_time = datetime.now().replace(microsecond=0)
        with open(log_path, "w") as fp:
            for c in cmds:
                fp.write(f"++ {c}\n")
                fp.flush()
                r = sub.run(c, shell=True, stdout=fp, stderr=fp, cwd=cwd, env=all_env)
                ret_code = r.returncode
                if ret_code != 0:
                    self.job_data["failed_cmd"] = c
                    self.update_status("ERROR")
                    self.dump_job_data()
                    return
        self.job_data["results"] = self.get_user_results()
        job_end_time = datetime.now().replace(microsecond=0)
        job_duration = str(job_end_time - job_start_time)
        self.job_data["job_duration"] = job_duration
        self.job_data["job_start_time"] = job_start_time
        self.update_status("DONE")
        self.dump_job_data()
    def get_user_results(self):
        cwd = self.job_data["cwd"]
        job_name = self.job_data["job_name"]
        user_result_file = f"{cwd}/se_user_result.yaml"
        user_results = {}
        if os.path.exists(user_result_file):
            with open(user_result_file) as fp:
                try:
                    user_results = yaml.safe_load(fp)
                    if not isinstance(user_results, dict):
                        user_results = {}
                        print(job_name, " user result yaml is not dict")
                except yaml.YAMLError as e:
                    user_results = {}
                    print(job_name, e)
        return user_results
    
    def job_table(self):
        job_data = self.job_data.copy()
        for target in ["envs", "results"]:
            if target in job_data:
                kv = job_data.pop(target)
                for k, v in kv.items():
                    t = target[:-1]
                    job_data[f"{t}/{k}"] = v
        return job_data
 
    def job_report(self):
        return self.job_data
