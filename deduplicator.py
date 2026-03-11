# deduplicator.py
# This file groups duplicate records together and merges them into one clean record.

from matcher import are_duplicates


def find_duplicate_groups(records):
    """
    Go through all records and group duplicates together.

    Example: if row 1 and row 2 are the same center,
    they'll end up in the same group.

    Returns a list of groups. Each group is a list of records.
    """

    # We'll track which group each record belongs to.
    # Start with each record in its own group.
    group_id = {}
    for r in records:
        group_id[r["id"]] = r["id"]  # each record starts in its own group

    def find_root(record_id):
        # Walk up the chain to find the group leader
        while group_id[record_id] != record_id:
            record_id = group_id[record_id]
        return record_id

    def merge_groups(id_a, id_b):
        # Join two groups together
        root_a = find_root(id_a)
        root_b = find_root(id_b)
        if root_a != root_b:
            group_id[root_b] = root_a

    # Compare every pair of records
    pair_results = []

    for i in range(len(records)):
        for j in range(i + 1, len(records)):
            a = records[i]
            b = records[j]

            result = are_duplicates(a, b)

            pair_results.append({
                "id_a": a["id"],
                "name_a": a.get("business_name"),
                "id_b": b["id"],
                "name_b": b.get("business_name"),
                "score": result["score"],
                "is_duplicate": result["is_duplicate"],
                "reasons": result["reasons"]
            })

            if result["is_duplicate"]:
                merge_groups(a["id"], b["id"])

    # Now collect records into their final groups
    groups = {}
    for r in records:
        root = find_root(r["id"])
        if root not in groups:
            groups[root] = []
        groups[root].append(r)

    # Convert dict to list
    all_groups = list(groups.values())

    return all_groups, pair_results


def merge_into_one_record(records_in_group, group_number):
    """
    Given a group of duplicate records, pick the best value
    for each field and combine them into one clean record.

    We prefer data from 'state_licensing' source first,
    then 'google_maps', then 'web_scrape'.
    """

    source_priority = ["state_licensing", "google_maps", "web_scrape", "crm"]

    # Sort records so the most trusted source comes first
    def source_rank(r):
        src = r.get("source", "")
        if src in source_priority:
            return source_priority.index(src)
        return 99

    sorted_records = sorted(records_in_group, key=source_rank)

    # These are all the fields we care about
    fields = [
        "business_name", "contact_name", "contact_email",
        "phone", "address", "city", "state", "zip",
        "license_number", "license_type", "license_issue_date",
        "capacity", "source"
    ]

    merged = {}
    field_sources = {}   # track where each field came from
    field_conflicts = {} # track if multiple values existed

    for field in fields:
        chosen_value = None
        chosen_from = None

        # Go through records in priority order, take first non-empty value
        for r in sorted_records:
            val = r.get(field)
            if val is not None and str(val).strip() not in ("", "None", "nan"):
                chosen_value = val
                chosen_from = f"row {r['id']} ({r.get('source', '?')})"
                break

        merged[field] = chosen_value
        field_sources[field] = chosen_from

        # Check for conflicts (did different records have different values?)
        all_values = []
        for r in sorted_records:
            v = r.get(field)
            if v is not None and str(v).strip() not in ("", "None", "nan"):
                all_values.append(str(v).strip())

        unique_values = list(set(all_values))
        if len(unique_values) > 1:
            field_conflicts[field] = unique_values

    merged_record = {
        "group_id": f"G-{group_number:03d}",
        "num_records_merged": len(records_in_group),
        "source_row_ids": [r["id"] for r in records_in_group],
        "data": merged,
        "field_sources": field_sources,
        "field_conflicts": field_conflicts,
        "needs_review": len(field_conflicts) > 0,
        "ai_enriched_fields": {}  # will be filled in by enricher.py
    }

    return merged_record


def run_deduplication(records):
    """
    Main function: takes a list of records and returns merged canonical records.
    """
    groups, pair_results = find_duplicate_groups(records)

    canonical_records = []
    for i, group in enumerate(groups):
        merged = merge_into_one_record(group, i + 1)
        canonical_records.append(merged)

    duplicates_found = len(records) - len(canonical_records)

    stats = {
        "total_input": len(records),
        "unique_entities": len(canonical_records),
        "duplicates_removed": duplicates_found,
        "needs_review": sum(1 for r in canonical_records if r["needs_review"])
    }

    return canonical_records, pair_results, stats
