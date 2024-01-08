#!/usr/bin/env bash

set -ex
export PS4='+${LINENO}: '

echo "TEST_VAR=123" > ./test
echo "TEST_VAR=123" >> ./test
echo "TEST_VAR=123" >> ./test

# sed command replace the content starting with 'TEST_VAR=' in the first line
sed -i -e '1s/TEST_VAR=.*/TEST_VAR=456\/jim/' ./test
# sed command replace the content starting with 'TEST_VAR=' in the second line
sed -i -e '2s/TEST_VAR=.*/TEST_VAR=456\/jim/' ./test


# sed command can't handle '/' in replacement string, so we need to escape it
replacement="TEST_VAR=2\/jim"
sed -i -e "2s/TEST_VAR=.*/${replacement}/" ./test

# sed command can use any character as delimiter, so we can use '|' instead of '/'
replacement="TEST_VAR=3/jim"
sed -i -e "3s|TEST_VAR=.*|${replacement}|" ./test

cat ./test

# rm ./test