import os
import json
import math
import pandas as pd
from deduper import find_duplicates, merge_cluster
from enricher import enrich_record


def load_excel(filepath):
    """Load the Excel file and return a list of row dicts."""
    df = pd.read_excel(filepath)
    df = df.dropna(axis=1, how="all")

    records = []
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            val = row[col]
            if isinstance(val, float) and math.isnan(val):
                val = None
            elif hasattr(val, "item"):
                val = val.item()
            if hasattr(val, "strftime"):
                try:
                    val = val.strftime("%Y-%m-%d")
                except:
                    val = None
            record[col] = val
        records.append(record)

    return records


def run(data_path, api_key=None, skip_enrichment=False):
    """
    Main pipeline function.
    Steps:
    1. Load data
    2. Find duplicates
    3. Merge each cluster of duplicates
    4. Enrich with Claude AI
    5. Save results
    """

    print(f"Loading data from {data_path}...")
    records = load_excel(data_path)
    print(f"Loaded {len(records)} records")

    print("Finding duplicates...")
    clusters, matched_pairs = find_duplicates(records)
    print(f"Found {len(clusters)} unique centers from {len(records)} records")
    print(f"Duplicate pairs found: {len(matched_pairs)}")

    print("Merging duplicates...")
    final_records = []
    for i, cluster in enumerate(clusters):
        merged_data, field_sources = merge_cluster(cluster)

        entry = {
            "id": f"CENTER-{i+1}",
            "merged_from_rows": [r["id"] for r in cluster],
            "num_sources": len(cluster),
            "needs_review": len(cluster) > 1,
            "data": merged_data,
            "field_sources": field_sources,
            "ai_enrichment": {}
        }
        final_records.append(entry)

    # AI enrichment runs by default using Claude API
    if not skip_enrichment:
        print(f"\nEnriching {len(final_records)} records with Claude AI...")
        enriched_count = 0
        for record in final_records:
            name = record['data'].get('business_name', record['id'])
            print(f"  Enriching: {name}...")
            enriched = enrich_record(record["data"], api_key)
            record["ai_enrichment"] = enriched
            if enriched and "error" not in enriched:
                enriched_count += 1
        print(f"  Successfully enriched {enriched_count}/{len(final_records)} records")
    else:
        print("Skipping AI enrichment (skip_enrichment=True)")

    # Save output
    os.makedirs("output", exist_ok=True)
    output = {
        "total_input_records": len(records),
        "total_unique_centers": len(final_records),
        "duplicates_merged": len(records) - len(final_records),
        "matched_pairs": matched_pairs,
        "centers": final_records
    }

    with open("output/results.json", "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\nResults saved to output/results.json")
    print(f"Summary: {len(records)} input → {len(final_records)} unique centers, {len(records)-len(final_records)} duplicates removed")
    return output


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "Book1.xlsx"
    # skip_enrichment flag for testing without API calls
    skip = "--skip-enrichment" in sys.argv
    run(path, skip_enrichment=skip)