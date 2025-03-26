# Big Data Analysis first assigment

First of all, create virtual env to not overload ypur pc with extra files 

```
python3 -m venv .venv                 
```
```
source .venv/bin/activate
```
```
pip install -r requirements.txt                                                               
```

Run the main script `gps_spoofing_detection.py -u [url]`

It doesn't use specific data, it works with any given `https://web.ais.dk/aisdata/` data.


The script is not at his finest, I really would like to upgrade it :(( but it is what it is.
At least it creates nice json file with all speed spoofing detections.
