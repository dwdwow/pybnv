import zipfile
import io

def unzip(data: bytes) -> bytes:
    """
    Unzip data from bytes and return the unzipped contents.
    
    Args:
        data: Bytes containing the zip file data
        
    Returns:
        The unzipped file contents as bytes
        
    Raises:
        zipfile.BadZipFile: If the data is not a valid zip file
    """
    with zipfile.ZipFile(io.BytesIO(data), 'r') as zip_ref:
        # Get the name of the first file in the archive
        first_file = zip_ref.namelist()[0]
        # Extract and return the contents
        return zip_ref.read(first_file)


def unzip_file(file_path: str) -> bytes:
    """
    Unzip file from disk and return the unzipped contents.
    
    Args:
        file_path: Path to the zip file
        
    Returns:
        The unzipped file contents as bytes
        
    Raises:
        zipfile.BadZipFile: If the file is not a valid zip file
        FileNotFoundError: If the file does not exist
    """
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        # Get the name of the first file in the archive
        first_file = zip_ref.namelist()[0]
        # Extract and return the contents
        return zip_ref.read(first_file)


def is_valid_zip(data: bytes) -> bool:
    """
    Check if the given bytes data represents a valid zip file.
    
    Args:
        data: Bytes containing the potential zip file data
        
    Returns:
        True if data is a valid zip file, False otherwise
    """
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zip_ref:
            # Test the zip file for corruption
            zip_ref.testzip()
            return True
    except zipfile.BadZipFile:
        return False



def is_valid_zip_file(file_path: str) -> bool:
    """
    Check if the given file path points to a valid zip file.
    
    Args:
        file_path: Path to the potential zip file
        
    Returns:
        True if file exists and is a valid zip file, False otherwise
    """
    try:
        with zipfile.ZipFile(file_path) as zip_ref:
            # Test the zip file for corruption
            zip_ref.testzip()
            return True
    except (zipfile.BadZipFile, FileNotFoundError):
        return False

