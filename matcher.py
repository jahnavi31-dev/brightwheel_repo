import re
from difflib import SequenceMatcher


def clean_name(name):
    if not name:
        return ""
    name = name.lower().strip()
    name = name.replace("ctr", "center")
    name = name.replace("child care", "childcare")
    name = name.replace("pre-school", "preschool")   # FIX: normalize hyphenated variant
    name = name.replace("day care", "daycare")         # FIX: normalize day care variant
    name = name.replace("llc", "").replace("inc", "").replace("corp", "")
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def clean_address(address):
    if not address:
        return ""
    address = address.lower().strip()
    address = address.replace(" st ", " street ")
    address = address.replace(" ave ", " avenue ")
    address = address.replace(" blvd ", " boulevard ")
    address = address.replace(" rd ", " road ")
    address = address.replace(" dr ", " drive ")
    address = address.replace(" ln ", " lane ")
    return address


def clean_phone(phone):
    if not phone:
        return ""
    return re.sub(r'\D', '', str(phone))


def similarity_score(text1, text2):
    if not text1 or not text2:
        return 0.0
    return SequenceMatcher(None, text1, text2).ratio()


def is_same_center(record1, record2):
    """
    Check if two records are the same real-world childcare center.
    Returns a score 0-1 and a reason string.
    """
    score = 0.0
    reasons = []

    # Phone number check - strongest signal
    phone1 = clean_phone(record1.get("phone", ""))
    phone2 = clean_phone(record2.get("phone", ""))
    if phone1 and phone2:
        if phone1 == phone2:
            score += 0.4
            reasons.append("same phone number")

    # Business name similarity
    name1 = clean_name(record1.get("business_name", ""))
    name2 = clean_name(record2.get("business_name", ""))
    name_sim = similarity_score(name1, name2)
    if name_sim > 0.8:
        score += 0.3
        reasons.append(f"similar name ({round(name_sim * 100)}% match)")
    elif name_sim > 0.6:
        score += 0.15

    # Address similarity
    addr1 = clean_address(record1.get("address", ""))
    addr2 = clean_address(record2.get("address", ""))
    addr_sim = similarity_score(addr1, addr2)
    if addr_sim > 0.8:
        score += 0.2
        reasons.append("same address")

    # ZIP code check
    zip1 = str(record1.get("zip", "")).strip()
    zip2 = str(record2.get("zip", "")).strip()
    if zip1 and zip2 and zip1 == zip2:
        score += 0.1
        reasons.append("same ZIP")

    # License number conflict - hard rule
    lic1 = str(record1.get("license_number", "") or "").strip()
    lic2 = str(record2.get("license_number", "") or "").strip()
    if lic1 and lic2 and lic1 != lic2:
        return 0.0, "different license numbers"

    # FIX: Boost score when name+address+ZIP are strong but one record has no phone.
    # name_sim>=0.90 + addr_sim>=0.80 + same ZIP is reliable evidence even without phone.
    if not phone1 or not phone2:
        if name_sim >= 0.90 and addr_sim >= 0.80 and zip1 and zip2 and zip1 == zip2:
            score = max(score, 0.70)
            reasons.append("high name+address match (no phone available)")

    reason_text = ", ".join(reasons) if reasons else "no strong match"
    return round(score, 2), reason_text