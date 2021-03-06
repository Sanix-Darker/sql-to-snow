# coding: utf-8
from flask import Flask, request, render_template
from flask_cors import CORS, cross_origin
import time

from werkzeug.utils import secure_filename

from app.utils import converter_box_sql, converter_box_snow

app = Flask(__name__)
CORS(app, support_credentials=True)

app.config['Secret'] = "Secret"


@app.route('/', methods=['GET'])  # To prevent Cors issues
@cross_origin(supports_credentials=True)
def index():
    return render_template("index.html")


@app.route('/convert-snow', methods=['POST'])  # To prevent Cors issues
@cross_origin(supports_credentials=True)
def converter():
    start = time.time()
    (qid,
     sql_query,
     snow_query) = converter_box_snow(request.json["sql_query"])

    elapsed = time.time() - start

    response = {
        "qid": qid,
        "sql_query": sql_query,
        "snow_query": snow_query,
        "elapsed": elapsed
    }

    return response


@app.route('/convert-sql', methods=['POST'])  # To prevent Cors issues
@cross_origin(supports_credentials=True)
def converter2():
    response = {}
    if request.method == 'POST':
        f = request.files['file']
        f.save(secure_filename(f.filename))

        start = time.time()
        qid, sql_query = converter_box_sql(f.filename)

        elapsed = time.time() - start

        response = {
            "qid": qid,
            "sql_query": sql_query,
            "elapsed": elapsed
        }

    return response


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=7777)
