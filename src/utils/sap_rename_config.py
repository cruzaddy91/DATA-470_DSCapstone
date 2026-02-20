"""
SAP table and column rename mappings for human-readable output.
Used when creating cleaned CSV files with obvious, readable names.
"""

import re

# -----------------------------------------------------------------------------
# Table name mappings: SAP short name -> readable filename (without .csv)
# -----------------------------------------------------------------------------
TABLE_NAME_MAP = {
    # Main tables
    "vbak": "sales_order_header",
    "vbap": "sales_order_item",
    "vbep": "sales_order_schedule_line",
    "vbfa": "sales_document_flow",
    "vbpa": "sales_document_partner",
    "vbrk": "billing_document_header",
    "vbrp": "billing_document_item",
    "vbuk": "sales_document_status_header",
    "vbup": "sales_document_status_item",
    "ekko": "purchase_order_header",
    "ekpo": "purchase_order_item",
    "ekbe": "purchase_order_history",
    "eket": "purchase_order_schedule",
    "likp": "delivery_header",
    "lips": "delivery_item",
    "mara": "material_master",
    "mard": "material_stock",
    "makt": "material_description",
    "kna1": "customer_master",
    "lfa1": "vendor_master",
    "konv": "pricing_condition",
    # Supporting tables
    "t001": "company_code",
    "t001w": "plant",
    "t002": "currency",
    "t006": "unit_of_measure",
    "t006a": "unit_of_measure_additional",
    "t006t": "unit_of_measure_text",
    "t009": "currency_code",
    "t009b": "currency_code_additional",
    "tvko": "sales_organization",
    "tvkot": "sales_organization_text",
    "tvtw": "sales_office",
    "tvtwt": "sales_office_text",
}

# -----------------------------------------------------------------------------
# Column name mappings: SAP abbreviation -> readable name
# Common across many tables; use lowercase keys for case-insensitive lookup
# -----------------------------------------------------------------------------
COLUMN_NAME_MAP = {
    # Client and organizational
    "mandt": "client_id",
    "bukrs": "company_code",
    "werks": "plant_code",
    "lgort": "storage_location",
    "vkorg": "sales_organization",
    "vtweg": "distribution_channel",
    "spart": "division",
    "vkbur": "sales_office",
    "vkgrp": "sales_group",
    "gsber": "business_area",
    # Sales document
    "vbeln": "sales_document_number",
    "posnr": "item_number",
    "etenr": "schedule_line_number",
    "vbtyp": "sales_document_type",
    "auart": "sales_document_type_code",
    "erdat": "created_date",
    "erzet": "created_time",
    "ernam": "created_by",
    "aedat": "changed_date",
    "aenam": "changed_by",
    # Customer and vendor
    "kunnr": "customer_number",
    "lifnr": "vendor_number",
    "kunag": "ship_to_customer",
    "parvw": "partner_function",
    "parnr": "partner_number",
    # Material
    "matnr": "material_number",
    "matwa": "material_number_customer",
    "matkl": "material_group",
    "arktx": "material_description",
    "maktx": "material_description_text",
    "mtart": "material_type",
    "meins": "base_unit_of_measure",
    "vrkme": "sales_unit",
    "charg": "batch_number",
    "prodh": "product_hierarchy",
    # Quantities and values
    "kwmeng": "cumulative_order_quantity",
    "lsmeng": "cumulative_confirmed_quantity",
    "lfimg": "delivery_quantity",
    "lgmng": "quantity_delivered",
    "menge": "quantity",
    "netwr": "net_value",
    "waerk": "currency_code",
    "waers": "currency_code",
    "netpr": "net_price",
    "peinh": "price_unit",
    "bpmng": "quantity_received",
    "rfmng": "referenced_quantity",
    "wmeng": "requested_quantity",
    "bmeng": "confirmed_quantity",
    "lmeng": "delivery_quantity_schedule",
    "smeng": "smallest_order_quantity",
    # Dates
    "audat": "order_date",
    "vdatu": "requested_delivery_date",
    "edatu": "requested_delivery_date_schedule",
    "lfdat": "loading_date",
    "wadat": "goods_issue_date",
    "ladtu": "delivery_date",
    "bedat": "purchasing_document_date",
    "bldat": "document_date",
    "fkdat": "billing_date",
    "bddat": "material_availability_date",
    "mbdat": "goods_receipt_date",
    "eindt": "item_delivery_date",
    "ersda": "creation_date",
    # Purchase order
    "ebeln": "purchase_order_number",
    "ebelp": "purchase_order_item_number",
    "bsart": "purchasing_document_type",
    "bstyp": "purchasing_document_category",
    "bednr": "requisition_number",
    # Delivery
    "vbelv": "preceding_document_number",
    "posnv": "preceding_item_number",
    "vgbel": "reference_document_number",
    "vgpos": "reference_item_number",
    "vgtyp": "reference_document_type",
    # Billing
    "fkart": "billing_type",
    "fktyp": "billing_category",
    "belnr": "accounting_document_number",
    "gjahr": "fiscal_year",
    "poper": "posting_period",
    "fkimg": "billed_quantity",
    "fklmg": "cumulative_billed_quantity",
    # Status
    "rfsta": "billing_status",
    "lfsta": "delivery_status",
    "wbsta": "goods_movement_status",
    "fksta": "billing_status_item",
    "besta": "goods_receipt_status",
    "gbsta": "invoice_receipt_status",
    "lstst": "delivery_status_item",
    "rfstk": "billing_status_header",
    "lfstk": "delivery_status_header",
    "wbstk": "goods_movement_status_header",
    # Address and location
    "land1": "country_code",
    "regio": "region",
    "ort01": "city",
    "stras": "street",
    "pstlz": "postal_code",
    "name1": "name_line_1",
    "name2": "name_line_2",
    "name3": "name_line_3",
    "name4": "name_line_4",
    "sortl": "search_term",
    "spras": "language_code",
    "adrnr": "address_number",
    # Conditions and pricing
    "knumv": "condition_record_number",
    "kposn": "condition_item_number",
    "stunr": "condition_step_number",
    "zaehk": "condition_counter",
    "kappl": "application",
    "kschl": "condition_type",
    "kbetr": "condition_rate",
    "kawrt": "condition_base_value",
    "kpein": "condition_price_unit",
    "kmein": "condition_unit",
    # Units and measures
    "butxt": "company_code_description",
    "msehi": "unit_of_measure",
    "msehl": "unit_of_measure_iso",
    # Misc
    "loekz": "deletion_indicator",
    "statu": "status",
    "pstyv": "item_category",
    "route": "route",
    "bstnk": "customer_reference",
    "recordstamp": "record_timestamp",
    "operation_flag": "operation_flag",
    "is_deleted": "is_deleted",
    # MARD stock
    "labst": "unrestricted_stock",
    "umlme": "returns",
    "insme": "stock_in_quality_inspection",
    "einme": "restricted_use_stock",
    "speme": "blocked_stock",
    "klabs": "unrestricted_stock_value",
    "lfgia": "fiscal_year",
    "lfmon": "fiscal_month",
    # EKBE/EKET
    "zekkn": "condition_item",
    "vgabe": "transaction_event",
    "buzei": "line_item_number",
    "bewtp": "valuation_type",
    "bwart": "movement_type",
    "budat": "posting_date",
    "dmbtr": "amount_in_local_currency",
    "wrbtr": "amount_in_document_currency",
    "lfgja": "fiscal_year_of_period",
    "lfbnr": "fiscal_period",
    "lfpos": "period_line_item",
    "grund": "reason_for_movement",
    # VBFa flow
    "posnn": "subsequent_item_number",
    "rfwrt": "referenced_value",
    "vbtyp_n": "subsequent_document_type",
    "vbtyp_v": "preceding_document_type",
}


def get_readable_column_name(sap_name: str) -> str:
    """
    Return a human-readable column name for an SAP field.
    Uses mapping if available; otherwise converts to snake_case.
    """
    if not sap_name or not isinstance(sap_name, str):
        return sap_name
    lower = sap_name.lower().strip()
    if lower in COLUMN_NAME_MAP:
        return COLUMN_NAME_MAP[lower]
    # Fallback: convert to snake_case (already usually lowercase with underscores)
    s = re.sub(r"[\s\-]+", "_", lower)
    s = re.sub(r"[^a-z0-9_]", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s if s else sap_name


def get_readable_table_filename(sap_table_name: str) -> str:
    """
    Return the readable filename (without .csv) for an SAP table.
    """
    base = sap_table_name.lower().replace(".csv", "").strip()
    return TABLE_NAME_MAP.get(base, base)
