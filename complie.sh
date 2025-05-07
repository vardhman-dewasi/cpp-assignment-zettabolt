#!/bin/env bash
clang++-14 $1 -o output
if [ $? -eq 0 ]; then
    ./output
fi