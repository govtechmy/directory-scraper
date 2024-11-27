"""
es_schema.py

This module defines the mapping between data categories (e.g., subdirectories in clean_data)
and their respective Elasticsearch indices. It also provides index-specific settings and
mappings, allowing for seamless dynamic indexing.

************************* Adding a New Data Category *************************

1. Add the category to `CATEGORY_INDEX_MAPPING`:
   - Key: The folder name under clean_data.
   - Value: The corresponding Elasticsearch index name.

2. Define the index settings and mappings in `INDEX_DEFINITIONS`:
   - Key: The Elasticsearch index name.
   - Value: A dictionary with "mappings".

3. Test the integration:
   - Ensure the category data is indexed into the correct Elasticsearch index.

******************************************************************************
"""
import os

ES_INDEX = os.getenv('ES_INDEX')

# ================================================================================
# ========================= INDEX MAPPINGS =======================================
# ================================================================================

ES_INDEX = {
    "mappings": {
        "properties": {
            "org_id": {"type": "keyword"},
            "org_name": {
                "type": "search_as_you_type",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "org_sort": {"type": "integer"},
            "org_type": {"type": "keyword"},
            "division_name": {
                "type": "search_as_you_type",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "division_sort": {"type": "integer"},
            "subdivision_name": {
                "type": "search_as_you_type",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "position_name": {"type": "search_as_you_type"},
            "person_name": {"type": "search_as_you_type"},
            "person_email": {"type": "keyword", "null_value": "NULL"},
            "person_fax": {"type": "keyword", "null_value": "NULL"},
            "person_phone": {"type": "keyword", "null_value": "NULL"},
            "position_sort": {"type": "integer"},
            "parent_org_id": {"type": "keyword", "null_value": "NULL"},
            "sha_256_hash": {"type": "keyword"},
        }
    }
}


BAHAGIAN_INDEX = {
    "mappings": {
        "properties": {
            "division_name": {"type": "text"},
            "subdivision_name": {"type": "text"},
            "function_description": {"type": "text"},
            "sha_256_hash": {"type": "keyword"},
        }
    }
}

# ======================= END OF INDEX SETTINGS AND MAPPINGS =====================

# ================================================================================
# ========================= INDEX NAMING  ========================================
# ================================================================================

INDEX_DEFINITIONS = {
    "directory": ES_INDEX,
    "bahagian-unit": BAHAGIAN_INDEX
}

# ======================= END OF INDEX NAMING ===================================


# ================================================================================
# =========================== CATEGORY-INDEX MAPPING =============================
# ================================================================================

CATEGORY_INDEX_MAPPING = {
    "ministry": "directory",
    "ministry_orgs": "directory",
    "non_ministry": "directory",
    "bahagian_unit": "bahagian-unit",
}

# ======================== END OF CATEGORY-INDEX MAPPING =========================

# ================================================================================
# ============================= UTILITY FUNCTIONS ================================
# ================================================================================

def get_index_for_category(category):
    """
    Get the Elasticsearch index name for a given category (subdirectory).
    """
    index_name = CATEGORY_INDEX_MAPPING.get(category)
    if not index_name:
        raise ValueError(f"No index mapped for category '{category}'.")
    return index_name


def get_index_definition(index_name):
    """
    Get the Elasticsearch index definition (settings and mappings) for a given index.
    """
    index_definition = INDEX_DEFINITIONS.get(index_name)
    if not index_definition:
        raise ValueError(f"No definition found for index '{index_name}'.")
    return index_definition

# =========================== END OF UTILITY FUNCTIONS ==========================
