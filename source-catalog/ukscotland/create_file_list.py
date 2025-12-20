with open('all_keys.txt') as f:
    lines = f.readlines()
    for line in lines:
        line = line.strip()
        if line.endswith('.tif') and '_DTM_' in line:
            key = line.split(' ')[-1]
            url = f'https://srsp-open-data.s3.eu-west-2.amazonaws.com/{key}'
            print(url)