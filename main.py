#!/usr/bin/env python3

from app.main import Main
from time import sleep
from os import path

ROOT_DIR = path.dirname(path.abspath(__file__))

if __name__ == '__main__':
    while True:
        Main().main()
        sleep(300)
