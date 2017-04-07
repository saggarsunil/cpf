#!/bin/bash

if [ $# -ne 1 ] ;
then
   echo "   Usage: ./change_location.sh [fridge_id1_1,fridge_id_2]"
   echo "   Example: ./change_location.sh 1,2"
  exit 1
fi

echo "Simualating change of location for iPork Fridge IDs: $1" 
echo "simulateFridgeMovement=$1" >>config.txt
