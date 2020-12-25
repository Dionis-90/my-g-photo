#!/bin/bash

SRV_ENDPOINT=https://photoslibrary.googleapis.com/v1/
API_KEY=AIzaSyCk1qpI9w87PqlS1SgJlwdroAGYqHgZEEs
OAUTH2_FILE_PTH=storage-photos.json
ACCESS_TOKEN=ya29.a0AfH6SMAYMLIPDagAvsqZfAH5R6EyH63hjWJUV4nGHGqyyfTUt7DxwJrJNSJCOsw2E8xn91iuHO1Og1Ic3-cym6vbFzWmN4s0tbKLqIMcb0ML6sK2pGAbfWClu_5xHDOWdadCO_1mWMAXSUZByftP8_UgYO0I8-WLfD3g
CLIENT_ID=$(jq -r '.client_id' $OAUTH2_FILE_PTH)
CLIENT_SECRET=$(jq -r '.client_secret' $OAUTH2_FILE_PTH)
REFRESH_TOKEN=$(jq -r '.refresh_token' $OAUTH2_FILE_PTH)
SCRIPT_NAME=$(basename "$0")
DB_PTH='db.sqlite'
MEDIA_OBJECTS_LIST=mediaItems

[ -z $API_KEY ] && echo "Api key is not found. Please set the API_KEY variable." && exit 1
[ -z "$CLIENT_ID" ] && echo "Client_id is not found. Please authenticate you and check the OAUTH2_FILE_PTH variable." && exit 1
if ! command -v sqlite3 > /dev/null; then
    echo "SQLite3 is not found. Please try sudo apt install sqlite3."
    exit 1
fi
[ ! -f $DB_PTH ] && echo "Database is not found. Please set the DB_PTH variable correct and create the sqlite DB." && exit 1
if ! command -v curl > /dev/null; then
    echo "CURL is not found. Please try sudo apt install curl."
    exit 1
fi

function refresh_token {
    NEW_ACCESS_TOKEN=$(curl --request POST \
    --data "client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&refresh_token=$REFRESH_TOKEN&grant_type=refresh_token" \
    https://accounts.google.com/o/oauth2/token \
    | jq -r .access_token)
    [ "$NEW_ACCESS_TOKEN" ] && ACCESS_TOKEN=$NEW_ACCESS_TOKEN &&
      sed -i "s/^ACCESS_TOKEN.*/ACCESS_TOKEN=$NEW_ACCESS_TOKEN/" "$SCRIPT_NAME" &&
      echo "Access token has been refreshed."
}

function get_list_one_page {
    OUTPUT_ARRAY=$(curl "${SRV_ENDPOINT}${MEDIA_OBJECTS_LIST}?key=$API_KEY&pageSize=10&pageToken=$NEXT_PAGE_TOKEN" \
    --header "Authorization: Bearer $ACCESS_TOKEN" --header 'Accept: application/json' --compressed)
    
    if echo "$OUTPUT_ARRAY" | grep 'UNAUTHENTICATED' > /dev/null; then
        refresh_token
        return 0
    fi
    PAGE=($(echo "$OUTPUT_ARRAY" | jq -r '.mediaItems[] | {id, filename, mimeType} | [.[]] | @csv'))
    NEXT_PAGE_TOKEN=$(echo "$OUTPUT_ARRAY" | jq -r '.nextPageToken')
    [ "$NEXT_PAGE_TOKEN" = "null" ] && echo "Invalid response, NEXT_PAGE_TOKEN is empty. Exiting..." && exit 1
    for i in "${PAGE[@]}"; do
        if echo "INSERT INTO my_media (object_id, filename, media_type) values ( $i );" | sqlite3 $DB_PTH
        then
            echo "$i - has been added to DB."
        else
            echo "Fail to add $i to DB."
            return 1
        fi
    done
}

while [ -z "$GET_FAIL" ]; do
    get_list_one_page || GET_FAIL=1
    sleep 2
done
