# views module for the flask web app

import sys
import os

sys.path.insert(0, os.getenv('HOME', 'default') + '/budgies/webui/')

from query_db import *
from flask import render_template, request
from webui import app

# base.html provides webpage header with title (linked to query form page) 
# and "about" link, which connects to presentation

@app.route('/')
def query():
	return render_template("query.html")

@app.route("/", methods=['POST'])
def query_post():
    query1 = request.form["query1"]
    query2 = request.form["query2"]
    query3 = request.form["query3"]
    query_response = send_query(query1)
    save_results = copy_to_s3(query2)
    send_results = send_email(query2, query3)

    return render_template("response.html")
