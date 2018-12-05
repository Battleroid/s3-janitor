# s3-janitor

He takes the buckets and empties out the dirty bleach water.

Given a bucket prefix or explicit buckets, this searches those buckets for objects whose last modified date exceeds the cutoff date.

## Usage

```
usage: janitor.py [-h] [--days DAYS] --access-key ACCESS_KEY --secret-key
                  SECRET_KEY --endpoint ENDPOINT
                  [--target-method {prefix,buckets}] [--prefix PREFIX]
                  [-b [BUCKETS [BUCKETS ...]]]
                  [--delete-method {list_then_delete,delete_every_page}] [-d]
                  [--dry-run]

optional arguments:
  -h, --help            show this help message and exit
  --days DAYS           days to keep
  --access-key ACCESS_KEY
                        s3 access key
  --secret-key SECRET_KEY
                        s3 secret key
  --endpoint ENDPOINT   endpoint url
  --target-method {prefix,buckets}
                        method of bucket selection
  --prefix PREFIX       bucket prefix to target
  -b [BUCKETS [BUCKETS ...]], --buckets [BUCKETS [BUCKETS ...]]
                        explicit buckets to target
  --delete-method {list_then_delete,delete_every_page}
                        deletion method
  -d, --debug           debug logging
  --dry-run             dry run mode, do not delete
```

It's possible to delete everything from a bucket by specifying the cutoff date as `--days "-1"`. This should reverse the direction of the timedelta, instead _adding_ days to the cutoff date.

Depending on the method you choose for `--target-method` you must use the appropriate option, `prefix` requires `--prefix`, `buckets` requires `--buckets`.

The deletion method can be tweaked. Boto3 paginates objects in chunks of 1000 objects during a listing, the default method `list_then_delete` is to obtain a full bucket listing, then delete all the objects in 1000 object chunks (the deletion function `client.delete_objects` only accepts 1000 objects at once).

The secondary option `delete_every_page` will queue up a page's worth of chunks and delete them before progressing to the next page. Worth considering if the bucket in question has an obscene amount of objects.

## Example Usage

```
$ python janitor.py --access-key <some access key> --secret-key <some secret key> --endpoint "https://s3.example.com" --target-method buckets -b my-bucket --dry-run    
[2018-05-24 15:07:27] s3-janitor [INFO    ]: DRY RUN MODE: No deletions will be made
[2018-05-24 15:07:27] s3-janitor [INFO    ]: Searching for objects before cutoff date of 2018-02-23
[2018-05-24 15:07:28] s3-janitor.my-bucket_worker [INFO    ]: Found 931 objects in chunk 1 for my-bucket
[2018-05-24 15:07:29] s3-janitor.my-bucket_worker [INFO    ]: Found 499 objects in chunk 2 for my-bucket
[2018-05-24 15:07:30] s3-janitor.my-bucket_worker [INFO    ]: Found 365 objects in chunk 3 for my-bucket
[2018-05-24 15:07:31] s3-janitor.my-bucket_worker [INFO    ]: Found 0 objects in chunk 4 for my-bucket
[2018-05-24 15:07:31] s3-janitor.my-bucket_worker [INFO    ]: Found 0 objects in chunk 5 for my-bucket
... lots of chunks ...
[2018-05-24 15:08:20] s3-janitor.my-bucket_worker [INFO    ]: Found 0 objects in chunk 94 for my-bucket
[2018-05-24 15:08:28] s3-janitor.my-bucket_worker [INFO    ]: Found 0 objects in chunk 95 for my-bucket
[2018-05-24 15:08:28] s3-janitor.my-bucket_worker [INFO    ]: Total objects queued for deletion 1795
[2018-05-24 15:08:28] s3-janitor.my-bucket_worker [INFO    ]: 2 chunks formed from 1795 objects
[2018-05-24 15:08:28] s3-janitor.my-bucket_worker [INFO    ]: Deleting chunk 1/2 (1000 objects) for my-bucket
[2018-05-24 15:08:28] s3-janitor.my-bucket_worker [INFO    ]: Deleting chunk 2/2 (795 objects) for my-bucket
[2018-05-24 15:08:28] s3-janitor.my-bucket_worker [INFO    ]: Finished deletions
```

If you're thinking this is slower than in another language like Go (just listing objects in Go was 2-3x quicker for example), you're correct, if you have a similar project written in Go, ping me so I can point to it from here.
