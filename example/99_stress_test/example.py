#!/bin/env python3
import shell_executor as se

jobs = {}
for i in range(10):
    jobs[f"{i}"] = {
        "envs": {"id": i},
        "cmds": ["echo hi"],
    }
    # if i > 0:
    #    jobs[f"{i}"]["dep"] = f"{i-1}"

a = se.Agent("ws", jobs, rerun_status=["DONE", "ERROR"])
a.launch_gui()
