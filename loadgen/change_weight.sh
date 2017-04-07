#!/bin/bash

if [ $# -ne 1 ] ;
then
   echo "   Usage: ./change_weight.sh [fridge_id1_1,fridge_id_2]"
   echo "   Example: ./change_weight.sh 1,2"
  exit 1
fi

echo "Simualating change of weight for iPork Fridge IDs: $1" 
echo "simulateChangeInWeight=$1" >>config.txt
