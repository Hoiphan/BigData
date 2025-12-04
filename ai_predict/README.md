# Usage Instructions

## Installation

```
pip install -r requirement.txt
python main.py
```

## Access FastAPI UI

Open in browser:

```
ip:8127/docs
```

## POST Request Body Example

```json
{
  "data": [
    [820, 18, 150, 55, 610],
    [830, 17, 152, 56, 620],
    [840, 16, 155, 58, 630],
    [850, 15, 158, 60, 640],
    [860, 14, 160, 62, 650]
  ]
}
```

feature_cols = [
'traffic_volume','avg_vehicle_speed', 'vehicle_count_cars','vehicle_count_trucks','vehicle_count_bikes'
]

## cURL Example

```
curl -X 'POST' \
  'localhost:8127/predict' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "data": [
    [820, 18, 150, 55, 610],
    [830, 17, 152, 56, 620],
    [840, 16, 155, 58, 630],
    [850, 15, 158, 60, 640],
    [860, 14, 160, 62, 650]
  ]
}'
```
