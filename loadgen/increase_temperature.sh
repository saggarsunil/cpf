#!/bin/bash

if [ $# -ne 1 ] ;
then
   echo "Usage: ./increase_temperature.sh [fridge_id1_1,fridfe_id_2]"
   echo "Example: ./increase_temperature.sh 1,2"
  exit 1
fi

fridge_ids=$1

echo "Simualating high temperature for iPork Fridge IDs: $1" 

echo "simulateFridgePowerOff=$1" >>config.txt
