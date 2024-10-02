from flask import Flask, request, jsonify
from pymongo import MongoClient
from bson.json_util import dumps

app = Flask(__name__)

# Replace with your actual MongoDB connection string
myclient = MongoClient("mongodb+srv://Rajendra:65_ODA4@cluster0.c4uzthx.mongodb.net/")
mydb = myclient['logistic_data']
collection = mydb['order_data']

@app.route('/filter', methods=['GET'])
def filter_documents():
    query = request.args.to_dict()
    # Convert query values to string if they are not already
    for key, value in query.items():
        query[key] = str(value)
    try:
        documents = collection.find(query)
        return dumps(documents), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/aggregate', methods=['POST'])
def aggregate_documents():
    pipeline = request.json
    try:
        result = collection.aggregate(pipeline)
        return dumps(result), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
