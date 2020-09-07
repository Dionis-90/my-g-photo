#!/bin/bash
SRV_ENDPOINT=https://photoslibrary.googleapis.com/v1/
GET_MEDIA_OBJS_LIST=mediaItems
API_KEY=AIzaSyCk1qpI9w87PqlS1SgJlwdroAGYqHgZEEs
ACCESS_TOKEN=ya29.a0AfH6SMCaRMSUWzuo0M3AiC-baVMa7JN5QxgGnr2Q8DCnAdSwDImSQewIVcHGvNXWL7k_HF9SSriAWjBZIwEcnmdW45xJgt8aTv_lQu_nQ2rp1IRS3ZQ-okKJWY2V5txxpxW0pPgs1OsYLB979zaQOwkrfvKkFwaEjD3T
CLIENT_ID=494476562589-aa549njeeh4t0d4nm8vodjtk46qsbt5e.apps.googleusercontent.com
CLIENT_SECRET=zx1zVy3AbXZBPmzKyAJDAn7M
REFRESH_TOKEN='1//0cKy28dHAAld4CgYIARAAGAwSNwF-L9Irm5js6-ymj8kiEYflIIfL1Ze5lgb5y6dbFJXqrbmH5dzvDZe_tDmd88YdX3NpX8iIBCg'
SCRIPT_NAME=`basename $0`

function refresh_token {
	NEW_ACCESS_TOKEN=`curl \
	--request POST \
	--data "client_id=$CLIENT_ID&client_secret=$CLIENT_SECRET&refresh_token=$REFRESH_TOKEN&grant_type=refresh_token" \
	https://accounts.google.com/o/oauth2/token \
	 | jq .access_token | cut -d\" -f2`
	#echo "$ACCESS_TOKEN"; exit
	[ $NEW_ACCESS_TOKEN ] && ACCESS_TOKEN=$NEW_ACCESS_TOKEN && sed -i "/^ACCESS_TOKEN/s/ACCESS_TOKEN.*/ACCESS_TOKEN=$NEW_ACCESS_TOKEN/" $SCRIPT_NAME &&
	echo "Access token has been refreshed."
}

function get_list_one_page {
	TMP_OUTPUT=`curl \
	"${SRV_ENDPOINT}mediaItems?key=$API_KEY&pageSize=10&pageToken=$NEXT_PAGE_TOKEN" \
	--header "Authorization: Bearer $ACCESS_TOKEN" --header 'Accept: application/json' --compressed`
	#echo "$TMP_OUTPUT"; exit
	if echo "$TMP_OUTPUT" | grep UNAUTHENTICATED > /dev/null; then
		refresh_token &&
		echo "Access token has been refreshed."
	fi
	PAGE=(`echo "$TMP_OUTPUT" \
	 | jq -r '.mediaItems[] | {id, filename, mimeType} | [.[]] | @csv'`)
	NEXT_PAGE_TOKEN=`echo "$TMP_OUTPUT" | jq '.nextPageToken' | cut -d\" -f2`
	[ $NEXT_PAGE_TOKEN = "null" ] && echo "Invalid response, NEXT_PAGE_TOKEN is empty. Exiting..." && exit 1
	#echo ${PAGE[@]}; echo $NEXT_PAGE_TOKEN; exit
	for i in ${PAGE[@]}; do
		if echo "insert into my_media (object_id, filename, media_type) values ( $i );" | sqlite3 db.sqlite
		then
			echo "$i - has been added to DB."
		else
			echo "Fail to add $i to DB."
			return 1
		fi
	done
}

while [ -z $GET_FAIL ]; do
	get_list_one_page || GET_FAIL=1
done
