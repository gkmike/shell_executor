#!/bin/env python3
import shell_executor as se

jobs = {
    "job_1":
    {
        "cmds":
        [
            "echo 123",
            "echo 345"
        ]
    },
    "job_2":
    {
        "envs":
        {
            "COUNT": 2,
            "AA": 2
        },
        "cmds":
        [
            "echo $AA",
            "echo ok"
        ]
    }
}

a = se.Agent("ws", jobs)
a.launch_gui()
