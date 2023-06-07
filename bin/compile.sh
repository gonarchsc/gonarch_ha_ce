#!/usr/bin/bash


pyinstaller -w -F --add-data "templates:templates" --add-data "static:static" app.py
pyinstaller --onefile --paths ../../core --name gui ../../gui/run.py
pyinstaller --onefile --paths ../../core --name api ../../api/run.py
pyinstaller --onefile --name core ../../core/core.py
pyinstaller --onefile --name check ../../core/check.py