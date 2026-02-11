from datetime import datetime
import os
import csv

def generate_csv_report(results, report_dir):
    """
    results: List[TestResult]
    returns: full path to generated CSV file
    """

    output_file = f"{report_dir}/validation_report.csv"

    with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)

        # Header
        writer.writerow([
            "table_name",
            "test_type",
            "status",
            "message"
        ])

        # Rows
        for r in results:
            writer.writerow([
                r.table_name,
                r.test_type,
                r.status,
                r.message
            ])

    return output_file
