# vortexa-take-home

## Next Steps
* Tests
* Logging/Metrics?


## Running locally
First ensure everything needed is installed
`pip install -r requirements.txt`
Then the code can be ran via..
`python src/land_storage.py`

## Testing
Some basic tests have been added in the tests dir which use pytest, these can be ran via..
`pytest`

## Docker
### Build
`docker image build -t vortexa-takehome .`

### Run
`docker run -v ./data:/data -v ./output:/output vortexa-takehome`