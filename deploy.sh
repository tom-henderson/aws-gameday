#!/bin/bash
aws s3 cp server.py s3://ugly-builders-code/server.py
aws s3api put-object-acl --bucket ugly-builders-code --key server.py --acl public-read