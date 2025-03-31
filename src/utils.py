import os
from pathlib import Path



def move_files(base_dir, target_dir):
    """
    Moves files with specific extensions from the base directory to the target directory.
    Removes files with other specific extensions from the base directory.
    Args:
        base_dir (str): The path to the base directory containing the files to be moved or removed.
        target_dir (str): The path to the target directory where the files should be moved.
    Supported file extensions for moving:
        - .pdf
        - .json
    Supported file extensions for removing:
        - .docx
        - .doc
        - .zip
        - .xlsx
        - .xls
    Raises:
        Exception: If there is an error moving or removing a file, an error message is printed.
    """

    base_path = Path(base_dir)
    target_path = Path(target_dir)
    os.makedirs(target_path, exist_ok=True)
    for file in base_path.iterdir():  # Iterates over Path objects
        if file.suffix in {".pdf", ".json"}:
            try:
                file.rename(target_path / file.name)
            except Exception as e:
                print(f"Error moving {file.name}: {e}")

        elif file.suffix in {".docx", ".doc", ".zip", ".xlsx", ".xls"}:
            print(f"Removing {file}")
            try:
                file.unlink()  # More intuitive than os.remove
            except Exception as e:
                print(f"Error removing {file.name}: {e}")
