#!/usr/bin/env python
# This code is Public Domain.
# Author: Michael Wirth

import sys, os.path

try:
    import boto.s3.connection
    from boto.s3.key import Key
    from boto.exception import S3ResponseError
except:
    print "boto library (http://code.google.com/p/boto/) for aws needs to be installed"
    sys.exit(1)

try:
    import awscreds
except:
    print "awscreds.py file needed with access and secret globals for aws access"
    sys.exit(1)

"""
Script for compacting aws logs for s3 access.
s3 can be configured to store logs for s3 access. Logs are also stored in s3
as files. Unfortunately, the number of log files is huge: apparently amazon
generates 216 small log files per day.

This script combines all log files for one day into one file, compresses them
with bzip2, re-upload such combined log file back to s3 and deletes original
log files.

How it works, roughly:
* download all logs for one day from s3 locally
* combine them into one file & gzip
* upload back to s3
* delete the original files
* repeat until there are no more logs to process
"""

s3BucketName = "dealini-test"   # Bucket name without trailing slash

g_s3conn = None
filenames = None


def s3connection():
    global g_s3conn
    if g_s3conn is None:
        g_s3conn = boto.s3.connection.S3Connection(awscreds.access, awscreds.secret, True)
    return g_s3conn


def make_case_insensitive(key):
    # Get filename permutations
    global filenames
    filenames = []
    filename = os.path.basename(key)
    path = os.path.dirname(key)

    filename_permutations(filename)

    b = s3connection().get_bucket(s3BucketName)
    config = b.get_website_configuration()
    suffix = config['WebsiteConfiguration']['IndexDocument']['Suffix']
    error_key = config['WebsiteConfiguration']['ErrorDocument']['Key']

    rules = boto.s3.website.RoutingRules()

    for fname in filenames:
        if fname == filename:
            continue
        
        newrule = boto.s3.website.RoutingRule.when(key_prefix=os.path.join(path, fname)).then_redirect(replace_key=key)
        rules.add_rule(newrule)

    b.configure_website(suffix=suffix, error_key=error_key, routing_rules=rules)


    #k = b.get_key(path)

    #k = k.copy(k.bucket.name, k.name, {'myKey': 'myValue'}, preserve_acl=True)

    # Fetch all files we have to match
    """
    all_keys = b.list(logsToCompact)
    for day_keys in gen_files_for_day(all_keys):
        process_day(day_keys)
        limit -= 1
        if limit <= 0:
            break
    print("Had to delete %d files of total size %d bytes" % (g_total_deleted, g_total_deleted_size))
    if 0 != g_uncompressed_size:
        saved = g_uncompressed_size - g_compressed_size
        saved_percent = float(100) * float(saved) / float(g_uncompressed_size)
        print("Compressed size: %d, uncompressed size: %d, saving: %d which is %.2f %%" % (g_compressed_size, g_uncompressed_size, saved, saved_percent))        
        """

def filename_permutations(filename, pos=0):
    if len(filename) == pos:
        filenames.append(filename)
    else:
        upper = filename[:pos] + filename[pos:pos+1].upper() + filename[pos+1:]
        lower = filename[:pos] + filename[pos:pos+1].lower() + filename[pos+1:]

        if upper != lower:
            filename_permutations(upper, pos+1)
            filename_permutations(lower, pos+1)
        else:
            filename_permutations(filename, pos+1)


def main():
    #filename_permutations("DE_App.jpg")
    #print filenames
    make_case_insensitive("de_app.jpg")

if __name__ == "__main__":
    main()
