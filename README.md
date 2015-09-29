# Import meetings from SQL Server to MySQL

## IMPORTANT NOTE!

This is being provided as an example of how to import meeting listings from an external data source to the TSML plugin.

This example will *not* work for you without modification; it was setup to import data to the TSML plugin from a legacy database that was years old. You'll have to modify the SQL Query and datasource to fit your needs.

While the destination will be MySQL, your source may not be SQL Server. This set up comes with Linux drivers for SQL Server and MySQL for CentOS 7.

## One time set up for your user:

Copy these three lines to /home/youruser/.bashrc at the end:

    source /opt/rh/python33/enable
    export WORKON_HOME=$HOME/.virtualenvs
    export PROJECT_HOME=$HOME/Dprojects
    source /usr/bin/virtualenvwrapper.sh

Then run .bashrc to activate:

    source ~/.bashrc

Create the virtualenv for the Python app drivers, and install:

    mkvirtualenv yourvenv
    pip install --allow-external mysql-connector-python -r requirements.txt

## To work on the virtualenv, and run the code, after the first time setup:

    workon yourvenv
    python import_sql.py
