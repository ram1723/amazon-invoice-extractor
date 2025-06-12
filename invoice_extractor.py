# invoice_extractor.py
# A Python script to extract structured data from Amazon and Flipkart PDF invoices.
# Supports output as a single combined Excel or individual files per invoice.

import pdfplumber
import re
import os
import pandas as pd
from typing import Dict, Any, List
import sys


def extract_text_from_pdf(pdf_path: str) -> str:
    """Extracts all text from the PDF using pdfplumber."""
    texts: List[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                texts.append(page_text)
    return "\n".join(texts)


def extract_tables_from_pdf(pdf_path: str) -> List[List[List[str]]]:
    """Extracts tables from PDF pages using pdfplumber."""
    tables: List[List[List[str]]] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_tables = page.extract_tables()
            for table in page_tables:
                tables.append(table)
    return tables


def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def parse_amazon_invoice(text: str, tables: List[List[List[str]]]) -> Dict[str, Any]:
    """Parses Amazon invoice text and tables to extract key fields and items."""
    data: Dict[str, Any] = {}
    # Patterns for header fields
    patterns = {
        "order_number": r"Order Number[:\s]*([A-Za-z0-9-]+)",
        "invoice_number": r"Invoice Number\s*[:\s]*([A-Za-z0-9-]+)",
        "order_date": r"Order Date[:\s]*([0-3]?\d[\.\-/][0-1]?\d[\.\-/][0-9]{4})",
        "invoice_date": r"Invoice Date\s*[:\s]*([0-3]?\d[\.\-/][0-1]?\d[\.\-/][0-9]{4})",
    }
    for field, pat in patterns.items():
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            data[field] = normalize_whitespace(m.group(1))
    # Seller, billing, shipping details
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if "Sold By" in line and "Billing Address" in line:
            parts = re.split(r"Sold By\s*:\s*", line, flags=re.IGNORECASE)
            if len(parts) > 1:
                rest = parts[1]
                subparts = re.split(r"Billing Address\s*:\s*", rest, flags=re.IGNORECASE)
                seller_part = subparts[0].strip()
                data["seller_details"] = normalize_whitespace(seller_part)
                if len(subparts) > 1:
                    billing_part = subparts[1].strip()
                    addr_lines = [billing_part]
                    for j in range(i+1, min(i+6, len(lines))):
                        if lines[j].strip() == "" or "Shipping Address" in lines[j]:
                            break
                        addr_lines.append(lines[j].strip())
                    data["billing_address"] = normalize_whitespace(" ".join(addr_lines))
        elif "Sold By" in line:
            seller = []
            for j in range(i+1, min(i+6, len(lines))):
                if lines[j].strip() == "" or re.search(r"Billing Address", lines[j], re.IGNORECASE):
                    break
                seller.append(lines[j].strip())
            if seller:
                data.setdefault("seller_details", normalize_whitespace(" ".join(seller)))
        if re.search(r"Billing Address", line, re.IGNORECASE) and "seller_details" not in data:
            billing = []
            for j in range(i+1, min(i+8, len(lines))):
                if lines[j].strip() == "" or re.search(r"Shipping Address", lines[j], re.IGNORECASE): break
                billing.append(lines[j].strip())
            if billing:
                data["billing_address"] = normalize_whitespace(" ".join(billing))
        if re.search(r"Shipping Address", line, re.IGNORECASE):
            shipping = []
            for j in range(i+1, min(i+8, len(lines))):
                if lines[j].strip() == "" or re.search(r"Order Number|Invoice Date", lines[j], re.IGNORECASE): break
                shipping.append(lines[j].strip())
            if shipping:
                data["shipping_address"] = normalize_whitespace(" ".join(shipping))
    # Parse line-items from tables
    items: List[Dict[str, Any]] = []
    for table in tables:
        if not table or not table[0]:
            continue
        header = [cell.lower() if cell else "" for cell in table[0]]
        if any("description" in h for h in header) and any(re.search(r"qty", h) for h in header):
            idx_map: Dict[str, int] = {}
            for idx, col in enumerate(header):
                if "description" in col:
                    idx_map["description"] = idx
                elif re.search(r"qty", col):
                    idx_map["quantity"] = idx
                elif re.search(r"unit price", col) or ("price" in col and "unit" in col):
                    idx_map["unit_price"] = idx
                elif re.search(r"net amount", col) or re.search(r"total amount", col) or re.search(r"amount", col):
                    idx_map["total_price"] = idx
            for row in table[1:]:
                if not any(cell for cell in row): continue
                item: Dict[str, Any] = {}
                if "description" in idx_map:
                    item["description"] = normalize_whitespace(row[idx_map["description"]] or "")
                if "quantity" in idx_map:
                    item["quantity"] = normalize_whitespace(row[idx_map["quantity"]] or "")
                if "unit_price" in idx_map:
                    item["unit_price"] = normalize_whitespace(row[idx_map["unit_price"]] or "")
                if "total_price" in idx_map:
                    item["total_price"] = normalize_whitespace(row[idx_map["total_price"]] or "")
                if item:
                    items.append(item)
    if items:
        data["items"] = items
    m_total = re.search(r"Total\s*Amount\s*[:\s]*[₹Rs\.]*\s*([0-9,]+\.?[0-9]*)", text, re.IGNORECASE)
    if m_total:
        data["total_amount"] = normalize_whitespace(m_total.group(1))
    return data


def parse_flipkart_invoice(text: str, tables: List[List[List[str]]]) -> Dict[str, Any]:
    """Parses Flipkart invoice text and tables to extract key fields."""
    data: Dict[str, Any] = {}
    patterns = {
        "order_id": r"Order ID[:\s]*([A-Za-z0-9-]+)",
        "invoice_number": r"Invoice Number[:\s]*([A-Za-z0-9-]+)",
        "invoice_date": r"Invoice Date[:\s]*([0-3]?\d[\.\-/][0-1]?\d[\.\-/][0-9]{4})",
        "issue_date": r"Issue Date[:\s]*([0-3]?\d[\.\-/][0-1]?\d[\.\-/][0-9]{4})",
    }
    for field, pat in patterns.items():
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            data[field] = normalize_whitespace(m.group(1))
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if re.search(r"Billing Address", line, re.IGNORECASE):
            billing = []
            for j in range(i+1, min(i+8, len(lines))):
                if lines[j].strip() == "" or re.search(r"Shipping Address", lines[j], re.IGNORECASE): break
                billing.append(lines[j].strip())
            if billing:
                data["billing_address"] = normalize_whitespace(" ".join(billing))
        if re.search(r"Shipping Address", line, re.IGNORECASE):
            shipping = []
            for j in range(i+1, min(i+8, len(lines))):
                if lines[j].strip() == "" or re.search(r"Order ID|Invoice Date", lines[j], re.IGNORECASE): break
                shipping.append(lines[j].strip())
            if shipping:
                data["shipping_address"] = normalize_whitespace(" ".join(shipping))
        if re.search(r"Sold By", line, re.IGNORECASE):
            seller = []
            for j in range(i+1, min(i+6, len(lines))):
                if lines[j].strip() == "": break
                seller.append(lines[j].strip())
            if seller:
                data["seller_details"] = normalize_whitespace(" ".join(seller))
    items: List[Dict[str, Any]] = []
    for table in tables:
        if not table or not table[0]: continue
        header = [cell.lower() if cell else "" for cell in table[0]]
        if any("description" in h or "item" in h for h in header) and any(re.search(r"qty", h) for h in header):
            idx_map: Dict[str, int] = {}
            for idx, col in enumerate(header):
                if "description" in col or "item" in col:
                    idx_map["description"] = idx
                elif re.search(r"qty", col):
                    idx_map["quantity"] = idx
                elif re.search(r"unit price", col) or ("price" in col and "unit" in col):
                    idx_map["unit_price"] = idx
                elif re.search(r"net amount", col) or re.search(r"total amount", col) or re.search(r"amount", col):
                    idx_map["total_price"] = idx
            for row in table[1:]:
                if not any(cell for cell in row): continue
                item: Dict[str, Any] = {}
                if "description" in idx_map:
                    item["description"] = normalize_whitespace(row[idx_map["description"]] or "")
                if "quantity" in idx_map:
                    item["quantity"] = normalize_whitespace(row[idx_map["quantity"]] or "")
                if "unit_price" in idx_map:
                    item["unit_price"] = normalize_whitespace(row[idx_map["unit_price"]] or "")
                if "total_price" in idx_map:
                    item["total_price"] = normalize_whitespace(row[idx_map["total_price"]] or "")
                if item:
                    items.append(item)
    if items:
        data["items"] = items
    m_total = re.search(r"Total[:\s]*[₹Rs\.]*\s*([0-9,]+\.?[0-9]*)", text, re.IGNORECASE)
    if m_total:
        data["total_amount"] = normalize_whitespace(m_total.group(1))
    return data


def detect_invoice_type(text: str) -> str:
    """Detect invoice type based on keywords in text."""
    if re.search(r"Amazon\.in|Sold By|Invoice Date", text, re.IGNORECASE):
        return "amazon"
    if re.search(r"Flipkart", text, re.IGNORECASE):
        return "flipkart"
    return "unknown"


def extract_invoice_to_dataframe(pdf_path: str) -> pd.DataFrame:
    """Extract invoice data and return a pandas DataFrame for items, including metadata."""
    text = extract_text_from_pdf(pdf_path)
    tables = extract_tables_from_pdf(pdf_path)
    inv_type = detect_invoice_type(text)
    if inv_type == "amazon":
        parsed = parse_amazon_invoice(text, tables)
    elif inv_type == "flipkart":
        parsed = parse_flipkart_invoice(text, tables)
    else:
        raise ValueError(f"Unknown invoice type for file {pdf_path}")
    items = parsed.get("items")
    metadata = {k: v for k, v in parsed.items() if k != "items"}
    if items:
        df = pd.DataFrame(items)
        for key, val in metadata.items():
            df[key] = val
    else:
        df = pd.DataFrame([metadata])
    for col in df.columns:
        if col in ("unit_price", "quantity", "total_price", "total_amount"):
            df[col] = df[col].astype(str).replace(r"[₹,]", "", regex=True)
            df[col] = pd.to_numeric(df[col], errors="ignore")
    return df


def process_folder(input_folder: str, output_path: str, combined: bool = False):
    """Process all PDF invoices in input_folder.
    If combined=True or output_path endswith .xlsx, write a single combined Excel.
    Else output_path is directory to save individual files and a combined file inside."""
    pdf_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.pdf')]
    all_dfs = []
    for fname in pdf_files:
        pdf_path = os.path.join(input_folder, fname)
        try:
            df = extract_invoice_to_dataframe(pdf_path)
            df['source_file'] = fname
            all_dfs.append(df)
        except Exception as e:
            print(f"Error processing {fname}: {e}")
    if not all_dfs:
        print("No invoices processed.")
        return
    combined_df = pd.concat(all_dfs, ignore_index=True)
    # Determine output behavior
    if combined or output_path.lower().endswith('.xlsx'):
        out_file = output_path if output_path.lower().endswith('.xlsx') else output_path + '.xlsx'
        parent = os.path.dirname(out_file)
        if parent and not os.path.exists(parent): os.makedirs(parent, exist_ok=True)
        combined_df.to_excel(out_file, index=False)
        print(f"Saved combined Excel: {out_file}")
    else:
        # output_path is directory
        os.makedirs(output_path, exist_ok=True)
        # Save individual files
        for df in all_dfs:
            fname = df.at[df.index[0], 'source_file']
            base = os.path.splitext(fname)[0]
            out_file = os.path.join(output_path, f"{base}.xlsx")
            df.drop(columns=['source_file'], inplace=False).to_excel(out_file, index=False)
            print(f"Saved {out_file}")
        # Save combined
        combined_file = os.path.join(output_path, 'combined_invoices.xlsx')
        combined_df.to_excel(combined_file, index=False)
        print(f"Saved combined Excel: {combined_file}")


def process_single_file(input_file: str, output_path: str):
    """Process a single PDF invoice and save to a specified output Excel file."""
    try:
        df = extract_invoice_to_dataframe(input_file)
    except Exception as e:
        print(f"Error processing {input_file}: {e}")
        return
    # Add source_file column
    fname = os.path.basename(input_file)
    df['source_file'] = fname
    # Determine output
    if output_path.lower().endswith('.xlsx'):
        out_file = output_path
    else:
        # treat as directory
        os.makedirs(output_path, exist_ok=True)
        base = os.path.splitext(fname)[0]
        out_file = os.path.join(output_path, f"{base}.xlsx")
    df.to_excel(out_file, index=False)
    print(f"Processed {input_file} -> {out_file}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Extract structured data from Amazon or Flipkart PDF invoices.")
    parser.add_argument("--input", required=True,
                        help="Path to folder containing invoice PDFs, or path to single PDF file.")
    parser.add_argument("--output", required=True,
                        help="Path to output Excel file for combined output, or folder for individual outputs.")
    parser.add_argument("--combined", action='store_true',
                        help="Force combined Excel output when input is a folder.")
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output
    if os.path.isfile(input_path) and input_path.lower().endswith('.pdf'):
        process_single_file(input_path, output_path)
    elif os.path.isdir(input_path):
        process_folder(input_path, output_path, combined=args.combined)
    else:
        print(f"Invalid input path: {input_path}. Must be a PDF file or a folder containing PDFs.")
        sys.exit(1)

if __name__ == '__main__':
    main()
