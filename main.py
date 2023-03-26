#!/usr/bin/env python3
from os import path, chdir
from app.main import Main
from time import sleep

ROOT_DIR = path.dirname(path.abspath(__file__))
chdir(ROOT_DIR)


if __name__ == '__main__':
    while True:
        Main().main()
        sleep(300)
