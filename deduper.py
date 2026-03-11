from matcher import is_same_center

MATCH_THRESHOLD = 0.65


def find_duplicates(records):
    """
    Compare every record against every other record.
    Group duplicates together into clusters.
    Returns a list of groups, where each group is a list of records.
    """
    total = len(records)
    matched_pairs = []

    # Compare all pairs and record which ones match
    for i in range(total):
        for j in range(i + 1, total):
            r1 = records[i]
            r2 = records[j]

            # only compare if same ZIP 
            zip1 = str(r1.get("zip", "")).strip()
            zip2 = str(r2.get("zip", "")).strip()
            if zip1 != zip2:
                continue

            score, reason = is_same_center(r1, r2)
            if score >= MATCH_THRESHOLD:
                matched_pairs.append({
                    "id_a": r1["id"],
                    "id_b": r2["id"],
                    "score": score,
                    "reason": reason
                })

    # Use union-find to group records into clusters
    # Think of it like a "which group does this belong to" lookup
    group_of = {r["id"]: r["id"] for r in records}

    def get_root(record_id):
        while group_of[record_id] != record_id:
            record_id = group_of[record_id]
        return record_id

    for pair in matched_pairs:
        root_a = get_root(pair["id_a"])
        root_b = get_root(pair["id_b"])
        if root_a != root_b:
            group_of[root_b] = root_a

    # Build final clusters
    clusters = {}
    for record in records:
        root = get_root(record["id"])
        if root not in clusters:
            clusters[root] = []
        clusters[root].append(record)

    return list(clusters.values()), matched_pairs


def merge_cluster(records_in_cluster):
    """
    Take a list of duplicate records and merge them into one best record.
    We prefer data from state_licensing, then google_maps, then web_scrape.
    """
    source_priority = ["state_licensing", "google_maps", "web_scrape", "crm"]

    def source_rank(record):
        source = record.get("source", "")
        if source in source_priority:
            return source_priority.index(source)
        return 99

    # Sort so highest priority source comes first
    sorted_records = sorted(records_in_cluster, key=source_rank)

    fields = [
        "business_name", "contact_name", "contact_email", "phone",
        "address", "city", "state", "zip", "license_number",
        "license_type", "license_issue_date", "capacity", "source"
    ]

    merged = {}
    field_sources = {}  # track where each field came from

    for field in fields:
        for record in sorted_records:
            value = record.get(field)
            if value is not None and str(value).strip() not in ("", "None", "nan"):
                merged[field] = value
                field_sources[field] = {
                    "value": value,
                    "from_row": record.get("id"),
                    "from_source": record.get("source", "unknown"),
                    "confirmed": True
                }
                break
        else:
            field_sources[field] = {
                "value": None,
                "from_row": None,
                "from_source": None,
                "confirmed": False
            }

    return merged, field_sources