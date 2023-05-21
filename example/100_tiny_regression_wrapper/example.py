#!/usr/bin/env python3
from tiny_regression_test import regression_test

rt = regression_test("test_ws")
test = rt.create_test("code_test")
build_job = test.create_job("build_code")
build_job.file.links.add("example/src/*")
build_job.file.copys.add("example/pat/*")
build_job.env.set("OPT", "-O2")
build_job.cmd.add("make")
build_job.cmd.add("test a.out")

sim_job = test.create_job("run_program")
sim_job.file.links.add(build_job.get_cwd() + '/*')
sim_job.cmd.add("make run | tee result.txt")
sim_job.cmd.add("diff golden.txt result.txt")
sim_job.env.set("STR", "123")

test2 = rt.create_test("code_test_no_opt")
build_job2 = test2.create_job("build_code")
build_job2.file.links.add("example/src/*")
build_job2.file.copys.add("example/pat/*")
build_job2.env.set("OPT", "-O0")
build_job2.cmd.add("make")
build_job2.cmd.add("test a.out")
sim_job2 = test2.create_job("run_program")
sim_job2.file.links.add(build_job2.get_cwd() + '/*')
sim_job2.cmd.add("make run | tee result.txt")
sim_job2.cmd.add("diff golden.txt result.txt")
sim_job2.env.set("STR", "123")

rt.process()
