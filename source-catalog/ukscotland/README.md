# ukscotland

List all S3 bucket keys with:

```
aws s3 ls s3://srsp-open-data/lidar/ --no-sign-request --recursive > all_keys.txt
```

Then extract the .tif DTM files with:

```
python3 create_file_list.py > file_list.txt
```