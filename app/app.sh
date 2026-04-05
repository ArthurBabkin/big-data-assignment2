#!/bin/bash

service ssh restart

bash start-services.sh

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt

scp requirements.txt cluster-slave-1:/tmp/requirements.txt
ssh cluster-slave-1 "pip3 install -r /tmp/requirements.txt"

bash prepare_data.sh

bash index.sh

bash search.sh "history of music"
bash search.sh "war and military conflict"
bash search.sh "science technology innovation"

sleep infinity
