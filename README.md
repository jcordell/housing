# Usage

Install requirements

```
pip install -r requirements.txt
```

Download files

```
python3 download.py
```

Generate data/article, takes several minutes to calculate all data.

```
python3 generate-indexhtml.py
```

If you change the article text/etc and don't want to re-run the full data analysis, run:

```
RECALCULATE=false python3 generate-indexhtml.py
```
