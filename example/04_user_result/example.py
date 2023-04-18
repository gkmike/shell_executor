#!/bin/env python3
import argparse
import shell_executor as se
import yaml

parser = argparse.ArgumentParser()
parser.add_argument('-y', '--yaml', type=str, required=True, help='input yaml for job description')
parser.add_argument('-c', '--max_concurrent', type=int, default=2, help='max concurrent workers(thread), default=2')
parser.add_argument('-w', '--work_space', type=str, default="./se_ws", help='specify the work space for launch jobs, default=./se_ws')
parser.add_argument('-o', '--output_csv', type=str, default="se_result.csv", help='specify the csv result')
args = parser.parse_args()

with open(args.yaml) as f:
    jobs = yaml.safe_load(f)
    a = se.Agent(args.work_space, jobs)
    a.run(args.max_concurrent)
    a.dump_csv(args.output_csv)