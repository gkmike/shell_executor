#!/bin/env python3
import shell_executor as se

jobs = {
    "job_1":
    {
        "cmds":
        [
            "echo hello world",
        ]
    },
    "job_2":
    {
        "envs":
        {
            "MY_STR": "hello world",
        },
        "cmds":
        [
            "echo $MY_STR",
        ]
    },
    "job_3":
    {
        "cmds":
        [
            "realpath @WD",
            "ls @WD",
        ]
    }
}

a = se.Agent("ws", jobs)
a.run(2)
a.dump_csv("se_result.csv")