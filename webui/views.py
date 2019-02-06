#The views module for the flask webapp.

#from callelastic import callelastic
from flask import render_template, request
from app import app

#App contains two pages: Query forms and Query results
#base.html provides webpage header with title (linked to query form page) and "about" link, which connects to presentation

@app.route('/')
def query():
	return render_template("query.html")

@app.route("/", methods=['POST'])
def query_post():
    query1 = request.form["query1"]
    query2 = request.form["query2"]
    query3 = request.form["query3"]
#    responselist = callelastic([query1, query2, query3])
#    return render_template("queryresponse.html", array1=responselist[0], array2=responselist[1])
    return render_template("queryresponse.html", array1=[], array2=[])
