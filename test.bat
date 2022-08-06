cls
call pip -v install .
call python test_modules.py
call pip uninstall -y dbacademy-gems --quiet