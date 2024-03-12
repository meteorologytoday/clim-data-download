#!/usr/bin/env python
from ecmwfapi import ECMWFDataServer

server = ECMWFDataServer()

server.retrieve({
    "class": "s2",
    "dataset": "s2s",
    "date": "2024-03-01",
    "expver": "prod",
    "hdate": "2002-03-01",
    "levtype": "o2d",
    "model": "glob",
    "origin": "cwao",
    "param": "151126/151131/151132/151145/151163/151175/151219/151225/174098",
    "step": "0-24",
    "stream": "enfh",
    "time": "00:00:00",
    "type": "cf",
    "target": "output_o2d"
})

