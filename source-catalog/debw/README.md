# debw

The download URLs were found with a script that looks roughly like this:

```python
import requests

def is_online(url):
    response = requests.head(url, allow_redirects=True, timeout=5)
    if response.status_code == 200:
        return True
    else:
        return False
    
# https://opengeodata.lgl-bw.de/data/dgm/dgm1_32_559_5424_2_bw.zip

if __name__ == "__main__":
    for horizontal in range(559, 609 + 2, 2):
        for vertical in range(5264, 5514 + 2, 2):
            url = f'https://opengeodata.lgl-bw.de/data/dgm/dgm1_32_{horizontal}_{vertical}_2_bw.zip'
            if is_online(url):
                print(url)
```