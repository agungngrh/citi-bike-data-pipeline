from pathlib import Path
from zipfile import ZipFile


def extract_tripdata(zip_path: Path, output_dir: Path) -> list[Path]:
    """Extract all trip CSV files from the ZIP archive."""
    output_dir.mkdir(parents=True, exist_ok=True)

    with ZipFile(zip_path) as archive:
        csv_files = [
            member for member in archive.namelist() if member.lower().endswith(".csv")
        ]

        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {zip_path.name}")

        extracted_files = []

        for member in csv_files:
            extracted_path = archive.extract(member, output_dir)
            extracted_files.append(Path(extracted_path))

    return extracted_files
