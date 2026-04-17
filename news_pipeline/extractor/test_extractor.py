import requests, trafilatura, json

url = "http://www.adaderana.lk/news.php?nid=121323"
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
response = requests.get(url, headers=headers, timeout=15)
result = trafilatura.extract(response.text, output_format='json')
print(json.dumps(json.loads(result), indent=2))