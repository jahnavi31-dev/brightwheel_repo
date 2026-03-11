import os
import json
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv

load_dotenv()  # reads the .env file and loads OPENAI_API_KEY into os.environ

app = Flask(__name__)

RESULTS_FILE = "output/results.json"
DATA_FILE = "data/Book1.xlsx"


def load_results():
    if os.path.exists(RESULTS_FILE):
        with open(RESULTS_FILE) as f:
            return json.load(f)
    return None


@app.route("/")
def home():
    results = load_results()
    return render_template("index.html", results=results)


@app.route("/run", methods=["POST"])
def run_pipeline():
    # API key comes from .env file only - never from the frontend
    api_key = os.environ.get("OPENAI_API_KEY", "")

    if not api_key:
        print("Warning: OPENAI_API_KEY not set in .env - skipping AI enrichment")

    try:
        from pipeline import run
        results = run(DATA_FILE, api_key=api_key or None)
        return jsonify({
            "success": True,
            "message": f"Done! Found {results['total_unique_centers']} unique centers from {results['total_input_records']} records."
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/results")
def get_results():
    results = load_results()
    if not results:
        return jsonify({"error": "No results yet, run the pipeline first"}), 404
    return jsonify(results)


if __name__ == "__main__":
    os.makedirs("output", exist_ok=True)
    print("Starting server at http://localhost:5000")
    app.run(debug=True, port=5000)
