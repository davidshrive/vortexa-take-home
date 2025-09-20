# vortexa-take-home

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

## Potential Improvements
* Optional debug logging
* Validation when loading/discharging cargo to ports
* Configuration values via arguments or enviroment variables
* More extensive testing
* Cargo events to a defined class and processing updated, would decouple cargo importing and processing
