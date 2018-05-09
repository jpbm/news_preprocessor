SEED_URLS='www.theonion.com' 
#SEED_URLS=$(cat ../crawler/SEED_URLS.txt)


for url in $SEED_URLS
do 
    python preprocessor.py $url &
done
