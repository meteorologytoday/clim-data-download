#!/usr/bin/env python
from ecmwfapi import ECMWFDataServer

server = ECMWFDataServer()

server.retrieve({
    "class": "s2",
    "dataset": "s2s",
    "date": "2024-03-01",
    "expver": "prod",
    "hdate": "2015-03-01",
    "levtype": "o2d",
    "model": "glob",
    "origin": "cwao",
    "param": "151131",
    "step": "72-96",
    "stream": "enfh",
    "time": "00:00:00",
    "type": "cf",
    "target": "output"
})
