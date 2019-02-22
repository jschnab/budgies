# views module for the flask web app

import sys
import os

sys.path.insert(0, os.getenv('HOME', 'default') + '/budgies/webui/')

from query_db import *
from flask import render_template, request
from webui import app
from threading import Thread

# base.html provides webpage header with title (linked to query form page) 
# and "about" link, which connects to presentation

def async_func(query1, query2, query3):
    """Perform Elasticsearch query on a new thread to avoid delaying \
response.html display."""
    thr = Thread(target=send_query, args=[query1, query2, query3])
    thr.start()
    return thr

@app.route('/')
def query():
	return render_template("query.html")

@app.route("/", methods=['POST'])
def query_post():
    query1 = request.form["query1"]
    query2 = request.form["query2"]
    query3 = request.form["query3"]
    thr = async_func(query1, query2, query3)

    return render_template("response.html")
