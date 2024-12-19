#!/bin/bash
echo "Starting Job Recommendation Engine"
cd /home/appuser
gunicorn --workers=1 -b 0.0.0.0:5000 jobrec_v2:app --log-file=-
