import requests
from typing import List
import xml.dom.minidom

def query_vision_xml_file_paths(prefix: str, marker: str = '') -> List[str]:
    """
    Query Binance Vision XML for a given prefix.
    
    Args:
        prefix: Prefix of the XML file to query
        if url is https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/,
        then prefix is data/spot/daily/aggTrades/BTCUSDT/

        marker: Start file path with prefix
        
    Returns:
        List of file paths
    """
    prefix = prefix.strip('/') + "/"
    url = f"https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix={prefix}&marker={marker}"
    response = requests.get(url)
    response.raise_for_status()
    
    encoding = response.encoding or 'utf-8'
    content = response.content.decode(encoding)
    
    el = xml.dom.minidom.parseString(content)
    
    contents = el.getElementsByTagName("Contents")

    file_paths: List[str] = []

    for content in contents:
        file_paths.append(content.getElementsByTagName("Key")[0].firstChild.data)
    
    next_marker_el = el.getElementsByTagName("NextMarker")

    next_marker = next_marker_el[0].firstChild.data if len(next_marker_el) > 0 else ""
    
    if next_marker == "":
        return file_paths
    
    other_file_paths = query_vision_xml_file_paths(prefix, next_marker)
    
    return file_paths + other_file_paths

if __name__ == "__main__":
    file_paths = query_vision_xml_file_paths("data/spot/daily/aggTrades/BTCUSDT/")
    print(file_paths)

